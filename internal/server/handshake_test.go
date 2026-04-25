package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandshakePacketBuild(t *testing.T) {
	t.Run("パケットのバイト列が正しい", func(t *testing.T) {
		// GIVEN
		nonce := make([]byte, 20)
		for i := range nonce {
			nonce[i] = byte(i + 1)
		}

		pkt := &handshakePacket{
			connectionId:     42,
			nonce:            nonce,
			serverCapability: serverCapability,
		}

		// WHEN
		buf := pkt.build()

		// THEN
		pos := 0

		// protocol_version
		assert.Equal(t, byte(protocolVersion), buf[pos])
		pos++

		// server_version (NULL 終端)
		ver, rest := readNullTermString(buf[pos:])
		assert.Equal(t, serverVersion, ver)
		pos += len(serverVersion) + 1

		// connection_id
		assert.Equal(t, uint32(42), readUint32(buf[pos:pos+4]))
		pos += 4

		// auth_plugin_data_part_1 (nonce[0:8])
		assert.Equal(t, nonce[0:8], buf[pos:pos+8])
		pos += 8

		// filler
		assert.Equal(t, byte(0x00), buf[pos])
		pos++

		// capability_flags_lower
		capLower := uint32(readUint16(buf[pos : pos+2]))
		pos += 2

		// character_set
		assert.Equal(t, byte(charsetUTF8MB4), buf[pos])
		pos++

		// status_flags
		assert.Equal(t, serverStatusAutocommit, readUint16(buf[pos:pos+2]))
		pos += 2

		// capability_flags_upper
		capUpper := uint32(readUint16(buf[pos:pos+2])) << 16
		pos += 2

		// capability の復元
		assert.Equal(t, serverCapability, capLower|capUpper)

		// auth_plugin_data_len
		assert.Equal(t, byte(21), buf[pos])
		pos++

		// reserved (10 バイト)
		assert.Equal(t, make([]byte, 10), buf[pos:pos+10])
		pos += 10

		// auth_plugin_data_part_2 (nonce[8:20] + NUL)
		assert.Equal(t, nonce[8:20], buf[pos:pos+12])
		pos += 12
		assert.Equal(t, byte(0x00), buf[pos])
		pos++

		// auth_plugin_name
		pluginName := readRestOfPacketString(buf[pos:])
		// NULL 終端なので末尾の 0x00 を除去
		assert.Equal(t, authPluginName+"\x00", pluginName)

		_ = rest
	})

	t.Run("packet interface を実装している", func(t *testing.T) {
		// GIVEN
		var p packet = &handshakePacket{
			nonce: make([]byte, 20),
		}

		// THEN
		assert.NotNil(t, p)
	})
}

func TestParseHandshakeResponse41(t *testing.T) {
	t.Run("正常なハンドシェイク応答をパースできる", func(t *testing.T) {
		// GIVEN: クライアントからのハンドシェイク応答を組み立てる (database 指定なし)
		capability := serverCapability &^ clientConnectWithDB
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

		// username (NULL 終端)
		buf = putNullTermString(buf, "root")

		// auth_response (clientSecureConnection: 1 バイト長さ + データ)
		scramble := make([]byte, 32)
		for i := range scramble {
			scramble[i] = byte(i)
		}
		buf = append(buf, byte(len(scramble)))
		buf = append(buf, scramble...)

		// auth_plugin_name (NULL 終端)
		buf = putNullTermString(buf, authPluginName)

		// WHEN
		resp, err := parseHandshakeResponse41(buf)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, capability, resp.capability)
		assert.Equal(t, uint32(16777216), resp.maxPacketSize)
		assert.Equal(t, uint8(charsetUTF8MB4), resp.characterSet)
		assert.Equal(t, "root", resp.username)
		assert.Equal(t, scramble, resp.authResponse)
		assert.Equal(t, authPluginName, resp.authPluginName)
	})

	t.Run("空パスワードの場合 authResponse が空になる", func(t *testing.T) {
		// GIVEN
		capability := serverCapability
		var buf []byte

		cap := make([]byte, 4)
		putUint32(cap, capability)
		buf = append(buf, cap...)

		maxPkt := make([]byte, 4)
		putUint32(maxPkt, 16777216)
		buf = append(buf, maxPkt...)

		buf = append(buf, charsetUTF8MB4)
		buf = append(buf, make([]byte, 23)...)
		buf = putNullTermString(buf, "root")

		// auth_response: 長さ 0
		buf = append(buf, 0)

		buf = putNullTermString(buf, authPluginName)

		// WHEN
		resp, err := parseHandshakeResponse41(buf)

		// THEN
		require.NoError(t, err)
		assert.Empty(t, resp.authResponse)
	})

	t.Run("CLIENT_PROTOCOL_41 がない場合エラーを返す", func(t *testing.T) {
		// GIVEN: clientProtocol41 を含まない capability
		capability := clientSecureConnection | clientPluginAuth
		var buf []byte

		cap := make([]byte, 4)
		putUint32(cap, capability)
		buf = append(buf, cap...)

		maxPkt := make([]byte, 4)
		putUint32(maxPkt, 16777216)
		buf = append(buf, maxPkt...)

		buf = append(buf, charsetUTF8MB4)
		buf = append(buf, make([]byte, 23)...)
		buf = putNullTermString(buf, "root")
		buf = append(buf, 0)
		buf = putNullTermString(buf, authPluginName)

		// WHEN
		_, err := parseHandshakeResponse41(buf)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "CLIENT_PROTOCOL_41")
	})

	t.Run("CLIENT_CONNECT_WITH_DB がある場合 database を読み飛ばす", func(t *testing.T) {
		// GIVEN
		capability := serverCapability | clientConnectWithDB
		var buf []byte

		cap := make([]byte, 4)
		putUint32(cap, capability)
		buf = append(buf, cap...)

		maxPkt := make([]byte, 4)
		putUint32(maxPkt, 16777216)
		buf = append(buf, maxPkt...)

		buf = append(buf, charsetUTF8MB4)
		buf = append(buf, make([]byte, 23)...)
		buf = putNullTermString(buf, "root")

		// auth_response
		buf = append(buf, 0)

		// database (NULL 終端)
		buf = putNullTermString(buf, "testdb")

		// auth_plugin_name
		buf = putNullTermString(buf, authPluginName)

		// WHEN
		resp, err := parseHandshakeResponse41(buf)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, "testdb", resp.database)
		assert.Equal(t, authPluginName, resp.authPluginName)
	})

	t.Run("CLIENT_SECURE_CONNECTION がない場合 authResponse が nil になる", func(t *testing.T) {
		// GIVEN: clientSecureConnection を含まない capability
		capability := clientProtocol41 | clientPluginAuth
		var buf []byte

		cap := make([]byte, 4)
		putUint32(cap, capability)
		buf = append(buf, cap...)

		maxPkt := make([]byte, 4)
		putUint32(maxPkt, 16777216)
		buf = append(buf, maxPkt...)

		buf = append(buf, charsetUTF8MB4)
		buf = append(buf, make([]byte, 23)...)
		buf = putNullTermString(buf, "root")

		// clientSecureConnection がないので auth_response の長さプレフィックスなし
		buf = putNullTermString(buf, authPluginName)

		// WHEN
		resp, err := parseHandshakeResponse41(buf)

		// THEN
		require.NoError(t, err)
		assert.Nil(t, resp.authResponse)
		assert.Equal(t, authPluginName, resp.authPluginName)
	})
}
