package server

import (
	"fmt"
	"io"
	"net"
)

// clientConn はパケットの読み書きとシーケンス ID の管理を担う
type clientConn struct {
	conn       net.Conn
	sequenceId uint8
}

func newClientConn(conn net.Conn) *clientConn {
	return &clientConn{
		conn:       conn,
		sequenceId: 0,
	}
}

// readPacket はパケットを読み込む
//
// パケット形式: [3 バイト LittleEndian ペイロード長][1 バイト シーケンス ID][ペイロード]
func (cc *clientConn) readPacket() ([]byte, error) {
	// ヘッダー (4 バイト) の読み込み
	header := make([]byte, 4)
	if _, err := io.ReadFull(cc.conn, header); err != nil {
		return nil, err
	}

	// ペイロード長 (3 バイト LittleEndian)
	payloadLen := readUint24(header[0:3])

	// シーケンス ID の検証
	seqId := header[3]
	if seqId != cc.sequenceId {
		return nil, fmt.Errorf("invalid sequence id: expected %d, got %d", cc.sequenceId, seqId)
	}
	cc.sequenceId++

	// ペイロードの読み込み
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(cc.conn, payload); err != nil {
			return nil, err
		}
	}

	return payload, nil
}

// writePacket はパケットを書き出す
//
// パケット形式: [3 バイト LittleEndian ペイロード長][1 バイト シーケンス ID][ペイロード]
func (cc *clientConn) writePacket(payload []byte) error {
	payloadLen := len(payload)

	// ヘッダー (4 バイト) + ペイロード
	packet := make([]byte, 4+payloadLen)

	// ペイロード長 (3 バイト LittleEndian)
	putUint24(packet[0:3], uint32(payloadLen))

	// シーケンス ID
	packet[3] = cc.sequenceId
	cc.sequenceId++

	// ペイロード
	copy(packet[4:], payload)

	_, err := cc.conn.Write(packet)
	return err
}

// resetSequenceId はシーケンス ID を 0 にリセットする
//
// Command Phase で各コマンドの開始時に呼び出す
func (cc *clientConn) resetSequenceId() {
	cc.sequenceId = 0
}
