package server

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/handler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatusFlags(t *testing.T) {
	t.Run("トランザクションなしの場合 autocommit を返す", func(t *testing.T) {
		// GIVEN
		s := &Server{}
		sess := newSession("", 0)

		// WHEN
		flags := s.statusFlags(sess)

		// THEN
		assert.Equal(t, serverStatusAutocommit, flags)
	})

	t.Run("トランザクション中の場合 in_trans を返す", func(t *testing.T) {
		// GIVEN
		s := &Server{}
		sess := newSession("", 0)
		sess.trxId = 1

		// WHEN
		flags := s.statusFlags(sess)

		// THEN
		assert.Equal(t, serverStatusInTrans, flags)
	})
}

func TestOnCommand(t *testing.T) {
	t.Run("COM_QUIT を受信すると終了する", func(t *testing.T) {
		// GIVEN
		serverConn, clientConn := createConnPair(t)
		sess := newSession("", 0)
		s := &Server{}

		done := make(chan struct{})
		go func() {
			s.onCommand(serverConn, sess)
			close(done)
		}()

		// WHEN: COM_QUIT を送信
		err := clientConn.writePacket([]byte{comQuit})
		require.NoError(t, err)

		// THEN: onCommand が終了する
		<-done
	})

	t.Run("COM_PING を受信すると OK_Packet を返す", func(t *testing.T) {
		// GIVEN
		serverConn, clientConn := createConnPair(t)
		sess := newSession("", 0)
		s := &Server{}

		done := make(chan struct{})
		go func() {
			s.onCommand(serverConn, sess)
			close(done)
		}()

		// WHEN: COM_PING を送信
		err := clientConn.writePacket([]byte{comPing})
		require.NoError(t, err)

		// THEN: OK_Packet が返る
		resp, err := clientConn.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0x00), resp[0]) // OK_Packet ヘッダー

		// クリーンアップ: COM_QUIT で終了
		clientConn.resetSequenceId()
		_ = clientConn.writePacket([]byte{comQuit})
		<-done
	})

	t.Run("未知のコマンドを受信すると ERR_Packet を返す", func(t *testing.T) {
		// GIVEN
		serverConn, clientConn := createConnPair(t)
		sess := newSession("", 0)
		s := &Server{}

		done := make(chan struct{})
		go func() {
			s.onCommand(serverConn, sess)
			close(done)
		}()

		// WHEN: 未知のコマンド (0xFF) を送信
		err := clientConn.writePacket([]byte{0xFF})
		require.NoError(t, err)

		// THEN: ERR_Packet が返る
		resp, err := clientConn.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0xFF), resp[0]) // ERR_Packet ヘッダー

		// クリーンアップ
		clientConn.resetSequenceId()
		_ = clientConn.writePacket([]byte{comQuit})
		<-done
	})

	t.Run("COM_PING の statusFlags がセッション状態を反映する", func(t *testing.T) {
		// GIVEN: トランザクション中のセッション
		serverConn, clientConn := createConnPair(t)
		sess := newSession("", 0)
		sess.trxId = 1
		s := &Server{}

		done := make(chan struct{})
		go func() {
			s.onCommand(serverConn, sess)
			close(done)
		}()

		// WHEN: COM_PING を送信
		err := clientConn.writePacket([]byte{comPing})
		require.NoError(t, err)

		// THEN: OK_Packet の status_flags が serverStatusInTrans
		resp, err := clientConn.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0x00), resp[0])
		// affected_rows (1 byte) + last_insert_id (1 byte) の後に status_flags
		assert.Equal(t, serverStatusInTrans, readUint16(resp[3:5]))

		// クリーンアップ
		clientConn.resetSequenceId()
		_ = clientConn.writePacket([]byte{comQuit})
		<-done
	})

	t.Run("COM_QUERY を受信すると handleComQuery に委譲する", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		serverConn, clientConn := createConnPair(t)
		sess := newSession("", 0)

		done := make(chan struct{})
		go func() {
			s.onCommand(serverConn, sess)
			close(done)
		}()

		// WHEN: COM_QUERY で CREATE TABLE を送信
		query := "CREATE TABLE test_cmd (id VARCHAR, PRIMARY KEY (id));"
		payload := append([]byte{comQuery}, []byte(query)...)
		err := clientConn.writePacket(payload)
		require.NoError(t, err)

		// THEN: OK_Packet が返る
		resp, err := clientConn.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0x00), resp[0])

		// クリーンアップ
		clientConn.resetSequenceId()
		_ = clientConn.writePacket([]byte{comQuit})
		<-done
	})
}
