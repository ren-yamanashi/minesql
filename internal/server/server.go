package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"minesql/internal/executor"
	"minesql/internal/parser"
	"minesql/internal/planner"
	"minesql/internal/storage"
	"net"
	"os"
	"strings"
	"time"
)

type Server struct {
	Address string
	Port    int
}

func NewServer(address string, port int) *Server {
	return &Server{
		Address: address,
		Port:    port,
	}
}

func (s *Server) Start() error {
	// ストレージマネージャーの初期化
	dataDir := "data"
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}
	err = os.Setenv("MINESQL_DATA_DIR", dataDir)
	if err != nil {
		return fmt.Errorf("failed to set environment variable MINESQL_DATA_DIR: %w", err)
	}
	storage.InitStorageManager()

	// ソケットを接続待ちに設定
	listenAddr := net.JoinHostPort(s.Address, fmt.Sprintf("%d", s.Port))
	tcpAddr, err := net.ResolveTCPAddr("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve address %s: %w", listenAddr, err)
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}
	defer listener.Close()

	// 接続の受付
	log.Printf("MineSQL Server started on %s", listenAddr)
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
	defer func() {
		log.Printf("Closing connection from %s", conn.RemoteAddr().String())
		conn.Close()
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
			if err != io.EOF {
				log.Printf("Read error: %v", err)
			}
			return
		}

		// exit なら切断
		if strings.ToLower(strings.TrimSpace(sql)) == "exit" {
			return
		}

		// クエリの実行
		result, err := s.executeQuery(sql)
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
func (s *Server) executeQuery(sql string) (string, error) {
	p := parser.NewParser()
	node, err := p.Parse(sql)
	if err != nil {
		return "", err
	}

	exec, err := planner.PlanStart(node)
	if err != nil {
		return "", err
	}

	records, err := executor.ExecutePlan(exec)
	if err != nil {
		return "", err
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

	// ヘッダーの作成
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, length)

	// ヘッダーとボディの書き込み
	packet := append(header, dataBytes...)
	_, err := conn.Write(packet)
	return err
}
