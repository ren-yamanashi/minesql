package server

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
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
	nextConnId     atomic.Uint32
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
// 接続フェーズ (Handshake → 認証) を経て、コマンドフェーズに移行する
//
// プロトコルの定義は docs/architecture/server/protocol/ を参照
func (s *Server) onConnection(conn *net.TCPConn) {
	cc := newClientConn(conn)

	defer func() {
		log.Printf("Closing connection from %s", conn.RemoteAddr().String())
		if err := conn.Close(); err != nil {
			log.Printf("failed to close connection: %v", err)
		}
	}()

	// --- 接続フェーズ ---

	// コネクション ID の採番
	connId := s.nextConnId.Add(1)

	// nonce (20 バイト) の生成
	nonce := make([]byte, 20)
	if _, err := rand.Read(nonce); err != nil {
		log.Printf("Failed to generate nonce: %v", err)
		return
	}

	// HandshakeV10 パケットの送信 (seq=0)
	hsPacket := &handshakePacket{
		connectionId:     connId,
		nonce:            nonce,
		serverCapability: serverCapability,
	}
	if err := cc.writePacket(hsPacket.build()); err != nil {
		log.Printf("Failed to send handshake: %v", err)
		return
	}

	// ハンドシェイク応答の受信 (seq=1)
	payload, err := cc.readPacket()
	if err != nil {
		log.Printf("Failed to read handshake response: %v", err)
		return
	}

	// ハンドシェイク応答のパース
	hsResp, err := parseHandshakeResponse41(payload)
	if err != nil {
		log.Printf("Failed to parse handshake response: %v", err)
		_ = cc.writePacket((&errPacket{
			errorCode: erUnknownError,
			sqlState:  sqlStateGeneralError,
			message:   err.Error(),
		}).build())
		return
	}

	// 認証
	if err := authenticate(hsResp.username, hsResp.authResponse, nonce); err != nil {
		_ = cc.writePacket((&errPacket{
			errorCode: erAccessDenied,
			sqlState:  sqlStateAuthError,
			message:   err.Error(),
		}).build())
		return
	}

	// AuthMoreData (fast auth success) の送信 (seq=2)
	if err := cc.writePacket((&authMoreDataPacket{statusByte: fastAuthSuccess}).build()); err != nil {
		log.Printf("Failed to send auth more data: %v", err)
		return
	}

	// OK_Packet の送信 (seq=3)
	if err := cc.writePacket((&okPacket{statusFlags: serverStatusAutocommit}).build()); err != nil {
		log.Printf("Failed to send OK after auth: %v", err)
		return
	}

	// セッションの生成
	sess := newSession(hsResp.username, hsResp.capability)
	defer func() {
		// アクティブなトランザクションがあれば自動ロールバック
		// (切断時に未コミットのトランザクションが残るとロックが解放されないため)
		if sess.trxId != 0 {
			if err := handler.Get().RollbackTrx(sess.trxId); err != nil {
				log.Printf("Auto rollback error: %v", err)
			}
		}
	}()

	// --- コマンドフェーズ ---
	for {
		cc.resetSequenceId()

		// タイムアウトの設定 (60 秒間何も送ってこなければ切断)
		if err := conn.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
			log.Printf("SetReadDeadline error: %v", err)
			return
		}

		// パケットの受信 (seq=0)
		cmdPayload, err := cc.readPacket()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("Read error: %v", err)
			}
			return
		}

		if len(cmdPayload) == 0 {
			continue
		}

		// コマンド種別の判定
		cmdType := cmdPayload[0]
		switch cmdType {
		case comQuit:
			return
		case comPing:
			_ = cc.writePacket((&okPacket{statusFlags: s.statusFlags(sess)}).build())
			continue
		case comQuery:
			// SQL 文字列は payload[1:] (コマンドバイトを除く)
			sql := string(cmdPayload[1:])
			s.handleComQuery(cc, sess, sql)
		default:
			_ = cc.writePacket((&errPacket{
				errorCode: 1047,
				sqlState:  sqlStateGeneralError,
				message:   "Unknown command",
			}).build())
		}
	}
}

// コマンド種別の定数
const (
	comQuit  byte = 0x01
	comQuery byte = 0x03
	comPing  byte = 0x0e
)

// handleComQuery は COM_QUERY を処理する
//
// 現時点では結果を旧プロトコルの CSV 形式で返す (Phase 3 で MySQL プロトコル形式に置き換え予定)。
func (s *Server) handleComQuery(cc *clientConn, sess *session, sql string) {
	result, err := s.executeQuery(sess, sql)
	if err != nil {
		_ = cc.writePacket((&errPacket{
			errorCode: erUnknownError,
			sqlState:  sqlStateGeneralError,
			message:   err.Error(),
		}).build())
		return
	}

	// 暫定: 結果を OK_Packet で返す (Phase 3 で結果セットに置き換え)
	_ = cc.writePacket((&okPacket{statusFlags: s.statusFlags(sess)}).build())
	_ = result // TODO: Phase 3 で結果セットのレスポンスに置き換え
}

// statusFlags はセッションの状態に応じた Server Status Flags を返す
func (s *Server) statusFlags(sess *session) uint16 {
	if sess.trxId != 0 {
		return serverStatusInTrans
	}
	return serverStatusAutocommit
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
