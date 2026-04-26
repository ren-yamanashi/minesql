package server

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"log"
	"net"

	"minesql/internal/storage/acl"
	"minesql/internal/storage/handler"
)

// onConnection は接続フェーズ (Handshake + 認証) を処理する
//
// 認証成功時は clientConn と session を返す。失敗時は nil を返す。
// プロトコルの定義は docs/architecture/server/protocol/ を参照。
func (s *Server) onConnection(conn *net.TCPConn) (*clientConn, *session) {
	cc := newClientConn(conn)

	// コネクション ID の採番
	connId := s.nextConnId.Add(1)

	// nonce (20 バイト) の生成
	nonce := make([]byte, 20)
	if _, err := rand.Read(nonce); err != nil {
		log.Printf("Failed to generate nonce: %v", err)
		return nil, nil
	}

	// ハンドシェイクパケットの送信 (seq=0)
	hsPacket := &handshakePacket{
		connectionId:     connId,
		nonce:            nonce,
		serverCapability: serverCapability,
	}
	if err := cc.writePacket(hsPacket.build()); err != nil {
		log.Printf("Failed to send handshake: %v", err)
		return nil, nil
	}

	// SSL 接続要求パケットの受信 (seq=1)
	sslPayload, err := cc.readPacket()
	if err != nil {
		log.Printf("Failed to read SSL connection request: %v", err)
		return nil, nil
	}

	// SSL 接続要求パケットは 32 バイト固定
	// 32 バイト以外の場合は TLS なし接続として拒否する
	if len(sslPayload) != 32 {
		log.Printf("Rejected non-TLS connection (payload size: %d)", len(sslPayload))
		if writeErr := cc.writePacket((&errPacket{
			errorCode: erUnknownError,
			sqlState:  sqlStateGeneralError,
			message:   "SSL connection is required",
		}).build()); writeErr != nil {
			log.Printf("Failed to send ERR packet: %v", writeErr)
		}
		return nil, nil
	}

	// TCP → TLS にアップグレード (シーケンス ID はそのまま維持)
	tlsConn := tls.Server(conn, s.tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		log.Printf("TLS handshake failed: %v", err)
		return nil, nil
	}
	cc.conn = tlsConn

	// ハンドシェイク応答の受信 (seq=2, TLS 上)
	payload, err := cc.readPacket()
	if err != nil {
		log.Printf("Failed to read handshake response: %v", err)
		return nil, nil
	}

	// ハンドシェイク応答のパース
	hsResp, err := parseHandshakeResponse41(payload)
	if err != nil {
		log.Printf("Failed to parse handshake response: %v", err)
		if writeErr := cc.writePacket((&errPacket{
			errorCode: erUnknownError,
			sqlState:  sqlStateGeneralError,
			message:   err.Error(),
		}).build()); writeErr != nil {
			log.Printf("Failed to send ERR packet: %v", writeErr)
		}
		return nil, nil
	}

	// 認証
	clientHost, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		log.Printf("Failed to parse remote address: %v", err)
		return nil, nil
	}

	hdl := handler.Get()
	result, authErr := authenticate(hdl.ACL, clientHost, hsResp.username, hsResp.authResponse, nonce)

	switch result {
	case authFailed:
		if writeErr := cc.writePacket((&errPacket{
			errorCode: erAccessDenied,
			sqlState:  sqlStateAuthError,
			message:   authErr.Error(),
		}).build()); writeErr != nil {
			log.Printf("Failed to send ERR packet: %v", writeErr)
		}
		return nil, nil

	case authSuccess:
		// AuthMoreData (fast auth success) の送信
		if err := cc.writePacket((&authMoreDataPacket{statusByte: fastAuthSuccess}).build()); err != nil {
			log.Printf("Failed to send auth more data: %v", err)
			return nil, nil
		}

	case authCacheMiss:
		// Complete Authentication
		if !s.completeAuth(cc, hdl, clientHost, hsResp.username) {
			return nil, nil
		}
	}

	// OK_Packet の送信
	if err := cc.writePacket((&okPacket{statusFlags: serverStatusAutocommit}).build()); err != nil {
		log.Printf("Failed to send OK after auth: %v", err)
		return nil, nil
	}

	return cc, newSession(hsResp.username, hsResp.capability)
}

// completeAuth は Complete Authentication (平文パスワード受信) を実行する
//   - 成功時は true を返し、Hash Entry Cache にエントリを追加する
//   - 失敗時は ERR パケットを送信して false を返す。
func (s *Server) completeAuth(cc *clientConn, hdl *handler.Handler, clientHost, username string) bool {
	// AuthMoreData (perform full auth) の送信
	if err := cc.writePacket((&authMoreDataPacket{statusByte: performFullAuth}).build()); err != nil {
		log.Printf("Failed to send perform full auth: %v", err)
		return false
	}

	// クライアントから平文パスワード + NUL 終端を受信
	passwordPayload, err := cc.readPacket()
	if err != nil {
		log.Printf("Failed to read password: %v", err)
		return false
	}

	// NUL 終端を除去
	if len(passwordPayload) > 0 && passwordPayload[len(passwordPayload)-1] == 0 {
		passwordPayload = passwordPayload[:len(passwordPayload)-1]
	}
	password := string(passwordPayload)

	// ユーザーの検索
	authString, ok := hdl.ACL.Lookup(clientHost, username)
	if !ok {
		if writeErr := cc.writePacket((&errPacket{
			errorCode: erAccessDenied,
			sqlState:  sqlStateAuthError,
			message:   "access denied for user '" + username + "'@'" + clientHost + "'",
		}).build()); writeErr != nil {
			log.Printf("Failed to send ERR packet: %v", writeErr)
		}
		return false
	}

	// ソルト付きハッシュと照合
	if !acl.VerifyCryptPassword(password, authString) {
		if writeErr := cc.writePacket((&errPacket{
			errorCode: erAccessDenied,
			sqlState:  sqlStateAuthError,
			message:   "access denied for user '" + username + "'@'" + clientHost + "'",
		}).build()); writeErr != nil {
			log.Printf("Failed to send ERR packet: %v", writeErr)
		}
		return false
	}

	// 認証成功: SHA256(SHA256(password)) を計算して Hash Entry Cache に保存
	stage1 := sha256.Sum256([]byte(password))
	stage2 := sha256.Sum256(stage1[:])
	hdl.ACL.SetHashEntry(username, stage2)

	return true
}
