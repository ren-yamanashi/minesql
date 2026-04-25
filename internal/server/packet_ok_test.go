package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOkPacketBuild(t *testing.T) {
	t.Run("affected_rows=0, last_insert_id=0, autocommit", func(t *testing.T) {
		// GIVEN
		pkt := &okPacket{
			affectedRows: 0,
			lastInsertId: 0,
			statusFlags:  serverStatusAutocommit,
		}

		// WHEN
		buf := pkt.build()

		// THEN
		assert.Equal(t, byte(0x00), buf[0])                           // ヘッダー
		assert.Equal(t, byte(0x00), buf[1])                           // affected_rows = 0
		assert.Equal(t, byte(0x00), buf[2])                           // last_insert_id = 0
		assert.Equal(t, serverStatusAutocommit, readUint16(buf[3:5])) // status_flags
		assert.Equal(t, uint16(0), readUint16(buf[5:7]))              // warnings = 0
		assert.Len(t, buf, 7)
	})

	t.Run("affected_rows=3, last_insert_id=0, in_trans", func(t *testing.T) {
		// GIVEN
		pkt := &okPacket{
			affectedRows: 3,
			lastInsertId: 0,
			statusFlags:  serverStatusInTrans,
		}

		// WHEN
		buf := pkt.build()

		// THEN
		assert.Equal(t, byte(0x00), buf[0]) // ヘッダー
		assert.Equal(t, byte(0x03), buf[1]) // affected_rows = 3
		assert.Equal(t, byte(0x00), buf[2]) // last_insert_id = 0
		assert.Equal(t, serverStatusInTrans, readUint16(buf[3:5]))
	})

	t.Run("affected_rows が大きい場合に長さエンコードされる", func(t *testing.T) {
		// GIVEN
		pkt := &okPacket{
			affectedRows: 1000,
			lastInsertId: 0,
			statusFlags:  serverStatusAutocommit,
		}

		// WHEN
		buf := pkt.build()

		// THEN
		assert.Equal(t, byte(0x00), buf[0]) // ヘッダー
		assert.Equal(t, byte(0xFC), buf[1]) // 長さエンコード: 3 バイト形式
	})
}

func TestOkPacketImplementsPacket(t *testing.T) {
	t.Run("packet interface を実装している", func(t *testing.T) {
		// GIVEN
		var p packet = &okPacket{}

		// THEN
		assert.NotNil(t, p)
	})
}
