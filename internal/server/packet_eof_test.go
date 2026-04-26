package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEofPacketBuild(t *testing.T) {
	t.Run("EOF_Packet を構築できる", func(t *testing.T) {
		// GIVEN
		pkt := &eofPacket{statusFlags: serverStatusAutocommit}

		// WHEN
		buf := pkt.build()

		// THEN
		assert.Equal(t, byte(0xFE), buf[0])                           // ヘッダー
		assert.Equal(t, uint16(0), readUint16(buf[1:3]))              // warnings = 0
		assert.Equal(t, serverStatusAutocommit, readUint16(buf[3:5])) // status_flags
		assert.Len(t, buf, 5)
	})

	t.Run("トランザクション中の status_flags が反映される", func(t *testing.T) {
		// GIVEN
		pkt := &eofPacket{statusFlags: serverStatusInTrans}

		// WHEN
		buf := pkt.build()

		// THEN
		assert.Equal(t, byte(0xFE), buf[0])
		assert.Equal(t, serverStatusInTrans, readUint16(buf[3:5]))
	})
}

func TestEofPacketImplementsPacket(t *testing.T) {
	t.Run("packet interface を実装している", func(t *testing.T) {
		// GIVEN
		var p packet = &eofPacket{}

		// THEN
		assert.NotNil(t, p)
	})
}
