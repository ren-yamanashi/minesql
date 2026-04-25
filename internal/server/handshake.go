package server

import "fmt"

const (
	protocolVersion = 10
	serverVersion   = "8.0.0-MineSQL"
	authPluginName  = "caching_sha2_password"
	charsetUTF8MB4  = 45 // utf8mb4_general_ci の collation ID
)

// handshakePacket は初期ハンドシェイクパケット (HandshakeV10) を表す
type handshakePacket struct {
	connectionId     uint32
	nonce            []byte // 20 バイトの認証データ
	serverCapability uint32
}

// build は初期ハンドシェイクパケットのペイロードを構築する
func (p *handshakePacket) build() []byte {
	var buf []byte

	// protocol_version: 10
	buf = append(buf, protocolVersion)

	// server_version: NULL 終端文字列
	buf = putNullTermString(buf, serverVersion)

	// connection_id: 4 バイト LittleEndian
	connId := make([]byte, 4)
	putUint32(connId, p.connectionId)
	buf = append(buf, connId...)

	// auth_plugin_data_part_1: nonce[0:8]
	buf = append(buf, p.nonce[0:8]...)

	// filler: 0x00
	buf = append(buf, 0x00)

	// capability_flags_lower: 下位 2 バイト
	capLower := make([]byte, 2)
	putUint16(capLower, uint16(p.serverCapability&0xFFFF))
	buf = append(buf, capLower...)

	// character_set: utf8mb4_general_ci
	buf = append(buf, charsetUTF8MB4)

	// status_flags: serverStatusAutocommit
	statusFlags := make([]byte, 2)
	putUint16(statusFlags, serverStatusAutocommit)
	buf = append(buf, statusFlags...)

	// capability_flags_upper: 上位 2 バイト
	capUpper := make([]byte, 2)
	putUint16(capUpper, uint16(p.serverCapability>>16))
	buf = append(buf, capUpper...)

	// auth_plugin_data_len: 21
	buf = append(buf, 21)

	// reserved: 10 バイトの 0x00
	buf = append(buf, make([]byte, 10)...)

	// auth_plugin_data_part_2: nonce[8:20] + NUL 終端 (13 バイト)
	buf = append(buf, p.nonce[8:20]...)
	buf = append(buf, 0x00)

	// auth_plugin_name: NULL 終端文字列
	buf = putNullTermString(buf, authPluginName)

	return buf
}

// handshakeResponse はクライアントからのハンドシェイク応答を表す
type handshakeResponse struct {
	capability     uint32
	maxPacketSize  uint32
	characterSet   uint8
	username       string
	authResponse   []byte // scramble (32 バイト)、空パスワードなら 0 バイト
	database       string // 空文字の場合あり
	authPluginName string
}

// parseHandshakeResponse41 はクライアントからのハンドシェイク応答をパースする
func parseHandshakeResponse41(payload []byte) (*handshakeResponse, error) {
	pos := payload

	// capability_flags (4 バイト)
	clientCap := readUint32(pos[:4])
	pos = pos[4:]

	// クライアントとサーバーの共通 capability
	commonCap := clientCap & serverCapability

	if commonCap&clientProtocol41 == 0 {
		return nil, fmt.Errorf("CLIENT_PROTOCOL_41 is required")
	}

	// max_packet_size (4 バイト)
	maxPacketSize := readUint32(pos[:4])
	pos = pos[4:]

	// character_set (1 バイト)
	characterSet := pos[0]
	pos = pos[1:]

	// reserved (23 バイト)
	pos = pos[23:]

	// username (NULL 終端)
	username, rest := readNullTermString(pos)
	pos = rest

	// auth_response
	var authResponse []byte
	if clientCap&clientSecureConnection != 0 {
		authLen := int(pos[0])
		pos = pos[1:]
		if authLen > 0 {
			authResponse = make([]byte, authLen)
			copy(authResponse, pos[:authLen])
			pos = pos[authLen:]
		}
	}

	// database
	var database string
	if clientCap&clientConnectWithDB != 0 {
		database, rest = readNullTermString(pos)
		pos = rest
	}

	// auth_plugin_name
	var pluginName string
	if clientCap&clientPluginAuth != 0 {
		pluginName, _ = readNullTermString(pos)
	}

	return &handshakeResponse{
		capability:     commonCap,
		maxPacketSize:  maxPacketSize,
		characterSet:   characterSet,
		username:       username,
		authResponse:   authResponse,
		database:       database,
		authPluginName: pluginName,
	}, nil
}
