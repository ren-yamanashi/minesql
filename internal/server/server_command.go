package server

import (
	"errors"
	"io"
	"log"
	"time"
)

// コマンド種別の定数
const (
	comQuit  byte = 0x01
	comQuery byte = 0x03
	comPing  byte = 0x0e
)

// onCommand は Command Phase のループを実行する
func (s *Server) onCommand(cc *clientConn, sess *session) {
	for {
		cc.resetSequenceId()

		// タイムアウトの設定 (1 時間何も送ってこなければ切断)
		if err := cc.conn.SetReadDeadline(time.Now().Add(1 * time.Hour)); err != nil {
			log.Printf("SetReadDeadline error: %v", err)
			return
		}

		// パケットの受信 (seq=0)
		payload, err := cc.readPacket()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("Read error: %v", err)
			}
			return
		}

		if len(payload) == 0 {
			continue
		}

		cmdType := payload[0]
		switch cmdType {
		case comQuit:
			return
		case comPing:
			_ = cc.writePacket((&okPacket{statusFlags: s.statusFlags(sess)}).build())
		case comQuery:
			s.onComQuery(cc, sess, string(payload[1:]))
		default:
			_ = cc.writePacket((&errPacket{
				errorCode: 1047,
				sqlState:  sqlStateGeneralError,
				message:   "Unknown command",
			}).build())
		}
	}
}

// statusFlags はセッションの状態に応じた Server Status Flags を返す
func (s *Server) statusFlags(sess *session) uint16 {
	if sess.trxId != 0 {
		return serverStatusInTrans
	}
	return serverStatusAutocommit
}
