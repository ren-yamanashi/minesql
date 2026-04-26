package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrPacketBuild(t *testing.T) {
	t.Run("パース エラーのパケットを構築できる", func(t *testing.T) {
		// GIVEN
		pkt := &errPacket{
			errorCode: erParseError,
			sqlState:  sqlStateSyntaxError,
			message:   "You have an error in your SQL syntax",
		}

		// WHEN
		buf := pkt.build()

		// THEN
		assert.Equal(t, byte(0xFF), buf[0])                                      // ヘッダー
		assert.Equal(t, erParseError, readUint16(buf[1:3]))                      // error_code
		assert.Equal(t, byte('#'), buf[3])                                       // sql_state_marker
		assert.Equal(t, sqlStateSyntaxError, string(buf[4:9]))                   // sql_state
		assert.Equal(t, "You have an error in your SQL syntax", string(buf[9:])) // message
	})

	t.Run("アクセス拒否のパケットを構築できる", func(t *testing.T) {
		// GIVEN
		pkt := &errPacket{
			errorCode: erAccessDenied,
			sqlState:  sqlStateAuthError,
			message:   "Access denied for user 'unknown'",
		}

		// WHEN
		buf := pkt.build()

		// THEN
		assert.Equal(t, byte(0xFF), buf[0])
		assert.Equal(t, erAccessDenied, readUint16(buf[1:3]))
		assert.Equal(t, "Access denied for user 'unknown'", string(buf[9:]))
	})
}

func TestErrPacketImplementsPacket(t *testing.T) {
	t.Run("packet interface を実装している", func(t *testing.T) {
		// GIVEN
		var p packet = &errPacket{}

		// THEN
		assert.NotNil(t, p)
	})
}
