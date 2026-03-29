package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"minesql/internal/ast"
	"minesql/internal/engine"
	"minesql/internal/executor"
	"minesql/internal/parser"
	"minesql/internal/planner"
	"minesql/internal/transaction"
	"minesql/internal/undo"
)

type Server struct {
	Address        string
	Port           int
	storageManager *engine.Engine
	undoLog        *undo.UndoLog
	trxManager     *transaction.Manager
}

func NewServer(address string, port int) *Server {
	undoLog := undo.NewUndoLog()
	return &Server{
		Address:    address,
		Port:       port,
		undoLog:    undoLog,
		trxManager: transaction.NewManager(undoLog),
	}
}

// サーバーを開始する
func (s *Server) Start() error {
	err := s.init()
	if err != nil {
		return fmt.Errorf("failed to initialize storage manager: %w", err)
	}

	listener, err := s.listen()
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listener.Addr().String(), err)
	}

	defer func() {
		if err := listener.Close(); err != nil {
			log.Printf("failed to close listener: %v", err)
		}
	}()

	s.accept(listener)
	return nil
}

// サーバーを停止する
func (s *Server) Stop() error {
	err := s.storageManager.BufferPool.FlushPage()
	if err != nil {
		return err
	}
	log.Println("All pages flushed successfully.")
	return nil
}

// サーバーの初期化
func (s *Server) init() error {
	dataDir := "data"
	err := os.MkdirAll(dataDir, 0750)
	if err != nil {
		return err
	}
	err = os.Setenv("MINESQL_DATA_DIR", dataDir)
	if err != nil {
		return err
	}
	s.storageManager = engine.Init()
	return nil
}

// サーバーソケットを接続待ちに設定して返す
func (s *Server) listen() (*net.TCPListener, error) {
	listenAddr := net.JoinHostPort(s.Address, fmt.Sprintf("%d", s.Port))
	tcpAddr, err := net.ResolveTCPAddr("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}
	return listener, nil
}

// クライアントからの接続を受け付ける
func (s *Server) accept(listener *net.TCPListener) {
	log.Printf("MineSQL Server started on %s", listener.Addr().String())
	for {
		conn, err := listener.AcceptTCP() // 保留状態の接続を取り出す
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue // 接続エラーになってもサーバーは落とさないので continue
		}
		log.Printf("New connection from %s", conn.RemoteAddr().String())
		go s.handleConnection(conn)
	}
}

// クライアントからの接続を処理する
// プロトコルの定義は ./docs/architecture/server.md#プロトコル を参照
func (s *Server) handleConnection(conn *net.TCPConn) {
	sess := newSession()

	defer func() {
		// アクティブなトランザクションがあれば自動ロールバック
		if sess.trxId != 0 {
			if err := s.trxManager.Rollback(sess.trxId); err != nil {
				log.Printf("Auto rollback error: %v", err)
			}
			sess.trxId = 0
		}
		log.Printf("Closing connection from %s", conn.RemoteAddr().String())
		if err := conn.Close(); err != nil {
			log.Printf("failed to close connection: %v", err)
		}
	}()

	for {
		// タイムアウトの設定 (10 分間何も送ってこなければ切断)
		err := conn.SetReadDeadline(time.Now().Add(10 * time.Minute))
		if err != nil {
			log.Printf("SetReadDeadline error: %v", err)
			return
		}

		// パケットの受信
		sql, err := s.readPacket(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("Read error: %v", err)
			}
			return
		}

		// exit なら切断
		if strings.ToLower(strings.TrimSpace(sql)) == "exit" {
			return
		}

		// クエリの実行
		result, err := s.executeQuery(sess, sql)
		if err != nil {
			err := s.writePacket(conn, fmt.Sprintf("Error: %v", err))
			if err != nil {
				log.Printf("Write error: %v", err)
			}
			continue
		}

		// 結果の送信
		if err := s.writePacket(conn, result); err != nil {
			log.Printf("Write error: %v", err)
			return
		}
	}
}

// クエリを実行して結果を文字列で返す
func (s *Server) executeQuery(sess *session, sql string) (string, error) {
	p := parser.NewParser()
	node, err := p.Parse(sql)
	if err != nil {
		return "", err
	}

	// トランザクション制御は planner を通さず直接処理する
	switch node.(type) {
	case *ast.BeginStmt:
		if sess.trxId != 0 {
			return "", fmt.Errorf("transaction already started")
		}
		sess.trxId = s.trxManager.Begin()
		return "", nil
	case *ast.CommitStmt:
		if sess.trxId == 0 {
			return "", fmt.Errorf("no active transaction")
		}
		s.trxManager.Commit(sess.trxId)
		sess.trxId = 0
		return "", nil
	case *ast.RollbackStmt:
		if sess.trxId == 0 {
			return "", fmt.Errorf("no active transaction")
		}
		if err := s.trxManager.Rollback(sess.trxId); err != nil {
			return "", err
		}
		sess.trxId = 0
		return "", nil
	}

	// トランザクション外の DML は autocommit (一時的な trxId を発行して即 Commit)
	autocommit := sess.trxId == 0
	trxId := sess.trxId
	if autocommit {
		trxId = s.trxManager.Begin()
	}

	exec, err := planner.Start(s.undoLog, trxId, node)
	if err != nil {
		if autocommit {
			_ = s.trxManager.Rollback(trxId)
		}
		return "", err
	}

	var records []executor.Record
	for {
		record, err := exec.Next()
		if err != nil {
			if autocommit {
				_ = s.trxManager.Rollback(trxId)
			}
			return "", err
		}
		if record == nil {
			break
		}
		records = append(records, record)
	}

	if autocommit {
		s.trxManager.Commit(trxId)
	}

	// 一旦、レスポンスは csv 形式で返す
	var msg strings.Builder
	for _, record := range records {
		line := make([]string, len(record))
		for i, col := range record {
			line[i] = string(col)
		}
		msg.WriteString(strings.Join(line, ",") + "\n")
	}
	return msg.String(), nil
}

// [Header 4 byte][Body N byte] を読み込む
func (s *Server) readPacket(conn *net.TCPConn) (string, error) {
	// ヘッダーの読み込み
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", err
	}

	// ボディの読み込み
	length := binary.BigEndian.Uint32(header)
	body := make([]byte, length)
	if _, err := io.ReadFull(conn, body); err != nil {
		return "", err
	}

	return string(body), nil
}

// [Header 4 byte][Body N byte] を書き込む
func (s *Server) writePacket(conn *net.TCPConn, msg string) error {
	dataBytes := []byte(msg)
	length := uint32(len(dataBytes))

	// パケットの作成 (先頭4バイトがヘッダー、続くバイトがボディ)
	packet := make([]byte, 4+len(dataBytes))
	binary.BigEndian.PutUint32(packet[0:4], length)
	copy(packet[4:], dataBytes)

	// パケットの書き込み
	_, err := conn.Write(packet)
	return err
}
