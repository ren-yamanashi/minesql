package server

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientConnReadWritePacket(t *testing.T) {
	t.Run("書き込んだパケットを読み取れる", func(t *testing.T) {
		// GIVEN
		server, client := createConnPair(t)

		// WHEN
		go func() {
			err := client.writePacket([]byte("hello"))
			assert.NoError(t, err)
		}()

		payload, err := server.readPacket()

		// THEN
		require.NoError(t, err)
		assert.Equal(t, []byte("hello"), payload)
	})

	t.Run("シーケンス ID が送受信で加算される", func(t *testing.T) {
		// GIVEN
		server, client := createConnPair(t)
		assert.Equal(t, uint8(0), client.sequenceId)
		assert.Equal(t, uint8(0), server.sequenceId)

		// WHEN: クライアントが seq=0 で書き込み → サーバーが読み取り
		go func() {
			err := client.writePacket([]byte("first"))
			assert.NoError(t, err)
		}()

		_, err := server.readPacket()
		require.NoError(t, err)

		// THEN: 両方の sequenceId が 1 に加算される
		assert.Equal(t, uint8(1), client.sequenceId)
		assert.Equal(t, uint8(1), server.sequenceId)
	})

	t.Run("複数パケットでシーケンス ID が連続する", func(t *testing.T) {
		// GIVEN
		server, client := createConnPair(t)

		// WHEN: 3 回の送受信
		for i := range 3 {
			go func() {
				err := client.writePacket([]byte{byte(i)})
				assert.NoError(t, err)
			}()

			payload, err := server.readPacket()
			require.NoError(t, err)
			assert.Equal(t, []byte{byte(i)}, payload)
		}

		// THEN: 両方とも sequenceId = 3
		assert.Equal(t, uint8(3), client.sequenceId)
		assert.Equal(t, uint8(3), server.sequenceId)
	})

	t.Run("不正なシーケンス ID でエラーになる", func(t *testing.T) {
		// GIVEN
		server, client := createConnPair(t)

		// クライアント側の sequenceId を進めて不一致にする
		client.sequenceId = 5

		// WHEN
		go func() {
			_ = client.writePacket([]byte("bad"))
		}()

		_, err := server.readPacket()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid sequence id")
	})

	t.Run("空のペイロードを送受信できる", func(t *testing.T) {
		// GIVEN
		server, client := createConnPair(t)

		// WHEN
		go func() {
			err := client.writePacket([]byte{})
			assert.NoError(t, err)
		}()

		payload, err := server.readPacket()

		// THEN
		require.NoError(t, err)
		assert.Equal(t, []byte{}, payload)
	})
}

func TestClientConnResetSequenceId(t *testing.T) {
	t.Run("シーケンス ID が 0 にリセットされる", func(t *testing.T) {
		// GIVEN
		server, client := createConnPair(t)

		go func() {
			err := client.writePacket([]byte("a"))
			assert.NoError(t, err)
		}()
		_, err := server.readPacket()
		require.NoError(t, err)

		assert.Equal(t, uint8(1), client.sequenceId)
		assert.Equal(t, uint8(1), server.sequenceId)

		// WHEN
		client.resetSequenceId()
		server.resetSequenceId()

		// THEN
		assert.Equal(t, uint8(0), client.sequenceId)
		assert.Equal(t, uint8(0), server.sequenceId)

		// リセット後に再度送受信できる
		go func() {
			err := client.writePacket([]byte("b"))
			assert.NoError(t, err)
		}()

		payload, err := server.readPacket()
		require.NoError(t, err)
		assert.Equal(t, []byte("b"), payload)
	})
}

// createConnPair はテスト用に接続されたペアの clientConn を作成する
func createConnPair(t *testing.T) (serverConn *clientConn, clientConn *clientConn) {
	t.Helper()
	s, c := net.Pipe()
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("failed to close server conn: %v", err)
		}
		if err := c.Close(); err != nil {
			t.Errorf("failed to close client conn: %v", err)
		}
	})
	return newClientConn(s), newClientConn(c)
}
