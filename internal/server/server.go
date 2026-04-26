package server

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync/atomic"

	"minesql/internal/storage/handler"
)

// InitUserOpts は初期ユーザーの設定
type InitUserOpts struct {
	Username string
	Host     string
}

type Server struct {
	address        string // IPアドレスまたはホスト名
	port           int    // ポート番号
	initUser       *InitUserOpts
	storageManager *handler.Handler
	nextConnId     atomic.Uint32
}

func NewServer(address string, port int, initUser *InitUserOpts) *Server {
	return &Server{
		address:  address,
		port:     port,
		initUser: initUser,
	}
}

// Start はサーバーを開始する
func (s *Server) Start() error {
	if err := s.init(); err != nil {
		return fmt.Errorf("failed to initialize: %w", err)
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
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return err
	}
	if err := os.Setenv("MINESQL_DATA_DIR", dataDir); err != nil {
		return err
	}
	s.storageManager = handler.Init()

	// ACL の初期化
	if err := s.initACL(); err != nil {
		return fmt.Errorf("failed to initialize ACL: %w", err)
	}

	return nil
}

// initACL はカタログからユーザーを読み込むか、初期ユーザーを作成して ACL を構築する
func (s *Server) initACL() error {
	hdl := handler.Get()

	if hdl.LoadACL() {
		if s.initUser != nil {
			log.Println("WARN: --init-user specified but user already exists in catalog, ignoring")
		}
		return nil
	}

	// カタログにユーザーがない場合は --init-* 引数が必要
	if s.initUser == nil {
		return fmt.Errorf("no user found in catalog; specify --init-user, --init-host on first startup")
	}

	password, err := hdl.CreateInitialUser(s.initUser.Username, s.initUser.Host)
	if err != nil {
		return err
	}

	log.Printf("Initial user '%s'@'%s' created with password: %s", s.initUser.Username, s.initUser.Host, password)
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
		go func() {
			var (
				cc   *clientConn
				sess *session
			)
			defer func() {
				if sess != nil && sess.trxId != 0 {
					if err := handler.Get().RollbackTrx(sess.trxId); err != nil {
						log.Printf("Auto rollback error: %v", err)
					}
				}
				log.Printf("Closing connection from %s", conn.RemoteAddr().String())
				if err := conn.Close(); err != nil {
					log.Printf("failed to close connection: %v", err)
				}
			}()

			cc, sess = s.onConnection(conn)
			if sess == nil {
				return
			}
			s.onCommand(cc, sess)
		}()
	}
}
