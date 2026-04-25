package server

import (
	"crypto/sha256"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"minesql/internal/storage/handler"
)

func TestOnConnection(t *testing.T) {
	t.Run("正常なハンドシェイクと認証で clientConn と session を返す", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()

		serverTCP, clientTCP := createTCPPair(t)

		type result struct {
			cc   *clientConn
			sess *session
		}
		resultCh := make(chan result, 1)
		go func() {
			cc, sess := s.onConnection(serverTCP)
			resultCh <- result{cc, sess}
		}()

		clientCC := newClientConn(clientTCP)

		// WHEN: ハンドシェイクを実行
		nonce := performClientHandshake(t, clientCC)
		_ = nonce

		// THEN: clientConn と session が返る
		r := <-resultCh
		assert.NotNil(t, r.cc)
		assert.NotNil(t, r.sess)
		assert.Equal(t, "root", r.sess.username)
	})

	t.Run("認証失敗時に nil を返す", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()

		serverTCP, clientTCP := createTCPPair(t)

		type result struct {
			cc   *clientConn
			sess *session
		}
		resultCh := make(chan result, 1)
		go func() {
			cc, sess := s.onConnection(serverTCP)
			resultCh <- result{cc, sess}
		}()

		clientCC := newClientConn(clientTCP)

		// WHEN: ハンドシェイクパケットを受信
		_, err := clientCC.readPacket()
		require.NoError(t, err)

		// 不正なユーザー名でハンドシェイク応答を送信
		hsResp := buildTestHandshakeResponse("unknown_user", []byte{})
		err = clientCC.writePacket(hsResp)
		require.NoError(t, err)

		// ERR_Packet を受信
		errPkt, err := clientCC.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0xFF), errPkt[0])
		assert.Equal(t, erAccessDenied, readUint16(errPkt[1:3]))

		// THEN: nil が返る
		r := <-resultCh
		assert.Nil(t, r.cc)
		assert.Nil(t, r.sess)
	})

	t.Run("コネクション ID がインクリメントされる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()

		// 1 回目の接続
		serverTCP1, clientTCP1 := createTCPPair(t)
		go func() { s.onConnection(serverTCP1) }()

		clientCC1 := newClientConn(clientTCP1)
		hs1, err := clientCC1.readPacket()
		require.NoError(t, err)

		pos1 := hs1[1:]
		_, pos1 = readNullTermString(pos1)
		connId1 := readUint32(pos1[:4])

		clientTCP1.Close()

		// 2 回目の接続
		serverTCP2, clientTCP2 := createTCPPair(t)
		go func() { s.onConnection(serverTCP2) }()

		clientCC2 := newClientConn(clientTCP2)
		hs2, err := clientCC2.readPacket()
		require.NoError(t, err)

		pos2 := hs2[1:]
		_, pos2 = readNullTermString(pos2)
		connId2 := readUint32(pos2[:4])

		// THEN: コネクション ID がインクリメントされている
		assert.Equal(t, connId1+1, connId2)

		clientTCP2.Close()
	})

	t.Run("accept から onConnection → onCommand の流れが正しく動作する", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()

		serverTCP, clientTCP := createTCPPair(t)

		done := make(chan struct{})
		go func() {
			defer func() {
				serverTCP.Close()
				close(done)
			}()
			cc, sess := s.onConnection(serverTCP)
			if sess == nil {
				return
			}
			defer func() {
				if sess.trxId != 0 {
					_ = handler.Get().RollbackTrx(sess.trxId)
				}
			}()
			s.onCommand(cc, sess)
		}()

		clientCC := newClientConn(clientTCP)

		// WHEN: ハンドシェイクを実行
		performClientHandshake(t, clientCC)

		// THEN: COM_PING が使える
		clientCC.resetSequenceId()
		err := clientCC.writePacket([]byte{comPing})
		require.NoError(t, err)

		pingResp, err := clientCC.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0x00), pingResp[0])

		// COM_QUIT で終了
		clientCC.resetSequenceId()
		_ = clientCC.writePacket([]byte{comQuit})
		<-done
	})
}

// performClientHandshake はテスト用にクライアント側のハンドシェイクを実行する
//
// ハンドシェイクパケットの受信 → 応答送信 → AuthMoreData 受信 → OK 受信を行い、nonce を返す
func performClientHandshake(t *testing.T, clientCC *clientConn) []byte {
	t.Helper()

	// ハンドシェイクパケットを受信
	hsPayload, err := clientCC.readPacket()
	require.NoError(t, err)

	// nonce を抽出
	pos := hsPayload[1:]
	_, pos = readNullTermString(pos) // server_version
	pos = pos[4:]                    // connection_id
	noncePart1 := pos[:8]
	pos = pos[8:]
	pos = pos[19:] // filler + cap_lower + charset + status + cap_upper + auth_data_len + reserved
	noncePart2 := pos[:12]

	nonce := make([]byte, 20)
	copy(nonce[:8], noncePart1)
	copy(nonce[8:], noncePart2)

	// ハンドシェイク応答を送信
	scramble := testComputeScramble("root", nonce)
	hsResp := buildTestHandshakeResponse("root", scramble)
	err = clientCC.writePacket(hsResp)
	require.NoError(t, err)

	// AuthMoreData パケットを受信
	authMore, err := clientCC.readPacket()
	require.NoError(t, err)
	assert.Equal(t, byte(0x01), authMore[0])
	assert.Equal(t, fastAuthSuccess, authMore[1])

	// OK パケットを受信
	okPkt, err := clientCC.readPacket()
	require.NoError(t, err)
	assert.Equal(t, byte(0x00), okPkt[0])

	return nonce
}

// createTCPPair はテスト用に接続された TCP コネクションのペアを作成する
func createTCPPair(t *testing.T) (serverConn *net.TCPConn, clientConn *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	accepted := make(chan *net.TCPConn, 1)
	go func() {
		conn, err := listener.AcceptTCP()
		if err != nil {
			return
		}
		accepted <- conn
	}()

	addr := listener.Addr().(*net.TCPAddr)
	client, err := net.DialTCP("tcp", nil, addr)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	server := <-accepted
	t.Cleanup(func() { server.Close() })

	return server, client
}

// buildTestHandshakeResponse はテスト用のハンドシェイク応答パケットを構築する
func buildTestHandshakeResponse(username string, scramble []byte) []byte {
	capability := serverCapability &^ clientConnectWithDB
	var buf []byte

	cap := make([]byte, 4)
	putUint32(cap, capability)
	buf = append(buf, cap...)

	maxPkt := make([]byte, 4)
	putUint32(maxPkt, 16777216)
	buf = append(buf, maxPkt...)

	buf = append(buf, charsetUTF8MB4)
	buf = append(buf, make([]byte, 23)...)
	buf = putNullTermString(buf, username)
	buf = append(buf, byte(len(scramble)))
	buf = append(buf, scramble...)
	buf = putNullTermString(buf, authPluginName)

	return buf
}

// testComputeScramble はテスト用の scramble を計算する
func testComputeScramble(password string, nonce []byte) []byte {
	stage1 := sha256.Sum256([]byte(password))
	stage2 := sha256.Sum256(stage1[:])

	h := sha256.New()
	h.Write(stage2[:])
	h.Write(nonce)
	digest := h.Sum(nil)

	scramble := make([]byte, 32)
	for i := range scramble {
		scramble[i] = stage1[i] ^ digest[i]
	}
	return scramble
}
