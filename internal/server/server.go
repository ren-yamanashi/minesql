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
	"minesql/internal/executor"
	"minesql/internal/parser"
	"minesql/internal/planner"
	"minesql/internal/storage/handler"
)

type Server struct {
	address        string // IPアドレスまたはホスト名
	port           int    // ポート番号
	storageManager *handler.Handler
}

func NewServer(address string, port int) *Server {
	return &Server{
		address: address,
		port:    port,
	}
}

// Start はサーバーを開始する
func (s *Server) Start() error {
	err := s.init()
	if err != nil {
		return fmt.Errorf("failed to initialize storage manager: %w", err)
	}

	listener, err := s.listen()
	if err != nil {
		return fmt.Errorf("failed to listen on %s:%d: %w", s.address, s.port, err)
	}

	defer func() {
		if err := listener.Close(); err != nil {
			log.Printf("failed to close listener: %v", err)
		}
	}()

	s.accept(listener)
	return nil
}

// Stop はサーバーを停止する
func (s *Server) Stop() error {
	if err := s.storageManager.Shutdown(); err != nil {
		return err
	}
	log.Println("All pages flushed and synced successfully.")
	return nil
}

// init はサーバーの初期化を行う
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
	s.storageManager = handler.Init()
	return nil
}

// listen はサーバーソケットを接続待ちに設定して返す
func (s *Server) listen() (*net.TCPListener, error) {
	listenAddr := net.JoinHostPort(s.address, fmt.Sprintf("%d", s.port))
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

// accept はクライアントからの接続を受け付ける
func (s *Server) accept(listener *net.TCPListener) {
	log.Printf("MineSQL Server started on %s", listener.Addr().String())
	for {
		conn, err := listener.AcceptTCP() // 保留状態の接続を取り出す
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue // 接続エラーになってもサーバーは落とさないので continue
		}
		log.Printf("New connection from %s", conn.RemoteAddr().String())
		go s.onConnection(conn)
	}
}

// onConnection はクライアントからの接続を処理する
//
// プロトコルの定義は docs/architecture/server/server.md#プロトコル を参照
func (s *Server) onConnection(conn *net.TCPConn) {
	session := newSession()

	defer func() {
		// アクティブなトランザクションがあれば自動ロールバック
		if session.trxId != 0 {
			if err := handler.Get().RollbackTrx(session.trxId); err != nil {
				log.Printf("Auto rollback error: %v", err)
			}
			session.trxId = 0
		}
		log.Printf("Closing connection from %s", conn.RemoteAddr().String())
		if err := conn.Close(); err != nil {
			log.Printf("failed to close connection: %v", err)
		}
	}()

	for {
		// タイムアウトの設定 (60 秒間何も送ってこなければ切断)
		err := conn.SetReadDeadline(time.Now().Add(60 * time.Second))
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
		result, err := s.executeQuery(session, sql)
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

// executeQuery はクエリを実行して結果を文字列で返す
func (s *Server) executeQuery(sess *session, sql string) (string, error) {
	p := parser.NewParser()
	node, err := p.Parse(sql)
	if err != nil {
		return "", err
	}

	// トランザクション制御は planner を通さず直接処理する
	if txStmt, ok := node.(*ast.TransactionStmt); ok {
		switch txStmt.Kind {
		case ast.TxBegin:
			if sess.trxId != 0 {
				return "", fmt.Errorf("transaction already started")
			}
			sess.trxId = handler.Get().BeginTrx()
			return "", nil
		case ast.TxCommit:
			if sess.trxId == 0 {
				return "", fmt.Errorf("no active transaction")
			}
			if err := handler.Get().CommitTrx(sess.trxId); err != nil {
				return "", err
			}
			sess.trxId = 0
			return "", nil
		case ast.TxRollback:
			if sess.trxId == 0 {
				return "", fmt.Errorf("no active transaction")
			}
			if err := handler.Get().RollbackTrx(sess.trxId); err != nil {
				return "", err
			}
			sess.trxId = 0
			return "", nil
		}
	}

	// トランザクション外の DML は autocommit (一時的な trxId を発行して即 Commit)
	hdl := handler.Get()
	autocommit := sess.trxId == 0
	trxId := sess.trxId
	if autocommit {
		trxId = hdl.BeginTrx()
	}

	// 実行計画の作成
	exec, err := planner.Start(trxId, node)
	if err != nil {
		if autocommit {
			_ = hdl.RollbackTrx(trxId)
		}
		return "", err
	}

	// クエリの実行
	var records []executor.Record
	for {
		record, err := exec.Next()
		if err != nil {
			if autocommit {
				_ = hdl.RollbackTrx(trxId)
			}
			return "", err
		}
		if record == nil {
			break
		}
		records = append(records, record)
	}

	if autocommit {
		if err := hdl.CommitTrx(trxId); err != nil {
			return "", err
		}
	}

	// レスポンスは csv 形式で返す
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

// readPacket は [Header 4 byte][Body N byte] を読み込む
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

// writePacket は [Header 4 byte][Body N byte] を書き込む
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
