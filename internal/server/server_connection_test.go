package server

import (
	"crypto/tls"
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
		defer func() {
			if err := serverTCP.Close(); err != nil {
				t.Errorf("failed to close serverTCP: %v", err)
			}
			if err := clientTCP.Close(); err != nil {
				t.Errorf("failed to close clientTCP: %v", err)
			}
		}()

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

		// WHEN: ハンドシェイクを実行 (TLS + Complete Auth)
		performClientHandshake(t, clientCC, clientTCP)

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
		defer func() {
			if err := serverTCP.Close(); err != nil {
				t.Errorf("failed to close serverTCP: %v", err)
			}
			if err := clientTCP.Close(); err != nil {
				t.Errorf("failed to close clientTCP: %v", err)
			}
		}()

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

		// SSL 接続要求パケットを送信
		sslReq := buildSSLConnectionRequest()
		err = clientCC.writePacket(sslReq)
		require.NoError(t, err)

		// TLS アップグレード
		tlsConn := tls.Client(clientTCP, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // テスト用自己署名証明書
		require.NoError(t, tlsConn.Handshake())
		clientCC.conn = tlsConn

		// 不正なユーザー名でハンドシェイク応答を送信
		// 存在しないユーザーなので authFailed が即座に返り ERR パケットが送信される
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

		if err := clientTCP1.Close(); err != nil {
			t.Logf("clientTCP1.Close: %v", err)
		}

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

		if err := clientTCP2.Close(); err != nil {
			t.Logf("clientTCP2.Close: %v", err)
		}
	})

	t.Run("accept から onConnection → onCommand の流れが正しく動作する", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()

		serverTCP, clientTCP := createTCPPair(t)

		done := make(chan struct{})
		go func() {
			defer func() {
				if err := serverTCP.Close(); err != nil {
					t.Logf("serverTCP.Close: %v", err)
				}
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

		// WHEN: ハンドシェイクを実行 (TLS + Complete Auth)
		performClientHandshake(t, clientCC, clientTCP)

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

	t.Run("TLS なし接続が拒否される", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()

		serverTCP, clientTCP := createTCPPair(t)
		defer func() {
			if err := clientTCP.Close(); err != nil {
				t.Logf("clientTCP.Close: %v", err)
			}
		}()

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

		// TLS なしでハンドシェイク応答を直接送信 (32 バイトではないので拒否される)
		hsResp := buildTestHandshakeResponse("root", []byte{})
		err = clientCC.writePacket(hsResp)
		require.NoError(t, err)

		// ERR パケットを受信
		errPkt, err := clientCC.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0xFF), errPkt[0])

		// THEN: nil が返る
		r := <-resultCh
		assert.Nil(t, r.cc)
		assert.Nil(t, r.sess)
	})
}

// performClientHandshake はテスト用にクライアント側のハンドシェイクを実行する
//
// ハンドシェイクパケット受信 → SSL 接続要求送信 → TLS アップグレード →
// ハンドシェイク応答送信 → Complete Auth (平文パスワード送信) → OK 受信を行う。
// clientCC.conn は TLS コネクションに差し替えられる。
func performClientHandshake(t *testing.T, clientCC *clientConn, rawConn net.Conn) {
	t.Helper()

	// ハンドシェイクパケットを受信 (seq=0)
	_, err := clientCC.readPacket()
	require.NoError(t, err)

	// SSL 接続要求パケットを送信 (seq=1)
	sslReq := buildSSLConnectionRequest()
	err = clientCC.writePacket(sslReq)
	require.NoError(t, err)

	// TLS アップグレード (シーケンス ID はそのまま維持)
	tlsConn := tls.Client(rawConn, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // テスト用自己署名証明書
	require.NoError(t, tlsConn.Handshake())
	clientCC.conn = tlsConn

	// ハンドシェイク応答を送信 (seq=2, TLS 上)
	// キャッシュが空なので scramble の内容は問わない (Complete Auth になる)
	hsResp := buildTestHandshakeResponse("root", make([]byte, 32))
	err = clientCC.writePacket(hsResp)
	require.NoError(t, err)

	// Complete Auth: AuthMoreData (0x04 = perform full auth) を受信
	authMore, err := clientCC.readPacket()
	require.NoError(t, err)
	assert.Equal(t, byte(0x01), authMore[0])
	assert.Equal(t, performFullAuth, authMore[1])

	// 平文パスワード + NUL 終端を送信
	err = clientCC.writePacket(append([]byte("root"), 0x00))
	require.NoError(t, err)

	// OK パケットを受信
	okPkt, err := clientCC.readPacket()
	require.NoError(t, err)
	assert.Equal(t, byte(0x00), okPkt[0])
}

// buildSSLConnectionRequest は SSL 接続要求パケット (32 バイト) を構築する
func buildSSLConnectionRequest() []byte {
	capability := serverCapability
	var buf []byte

	// capability_flags (4 バイト)
	cap := make([]byte, 4)
	putUint32(cap, capability)
	buf = append(buf, cap...)

	// max_packet_size (4 バイト)
	maxPkt := make([]byte, 4)
	putUint32(maxPkt, 16777216)
	buf = append(buf, maxPkt...)

	// character_set (1 バイト)
	buf = append(buf, charsetUTF8MB4)

	// reserved (23 バイト)
	buf = append(buf, make([]byte, 23)...)

	return buf // 4 + 4 + 1 + 23 = 32 バイト
}

// createTCPPair はテスト用に接続された TCP コネクションのペアを作成する
//
// 呼び出し元がコネクションのクローズを管理する
func createTCPPair(t *testing.T) (serverConn *net.TCPConn, clientConn *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	defer func() {
		if err := listener.Close(); err != nil {
			t.Errorf("failed to close listener: %v", err)
		}
	}()

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

	server := <-accepted
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
