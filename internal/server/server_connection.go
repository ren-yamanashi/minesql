package server

import (
	"crypto/rand"
	"log"
	"net"

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

	// ハンドシェイク応答の受信 (seq=1)
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
	if err := authenticate(handler.Get().ACL, clientHost, hsResp.username, hsResp.authResponse, nonce); err != nil {
		if writeErr := cc.writePacket((&errPacket{
			errorCode: erAccessDenied,
			sqlState:  sqlStateAuthError,
			message:   err.Error(),
		}).build()); writeErr != nil {
			log.Printf("Failed to send ERR packet: %v", writeErr)
		}
		return nil, nil
	}

	// AuthMoreData (fast auth success) の送信 (seq=2)
	if err := cc.writePacket((&authMoreDataPacket{statusByte: fastAuthSuccess}).build()); err != nil {
		log.Printf("Failed to send auth more data: %v", err)
		return nil, nil
	}

	// OK_Packet の送信 (seq=3)
	if err := cc.writePacket((&okPacket{statusFlags: serverStatusAutocommit}).build()); err != nil {
		log.Printf("Failed to send OK after auth: %v", err)
		return nil, nil
	}

	return cc, newSession(hsResp.username, hsResp.capability)
}
