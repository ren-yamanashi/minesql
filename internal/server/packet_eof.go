package server

// eofPacket は EOF_Packet を表す
//
// CLIENT_DEPRECATE_EOF が無効な場合に使用する
//
// 構造
//   - 0xFE (ヘッダー)
//   - warnings (2 バイト、常に 0)
//   - status_flags (2 バイト)
type eofPacket struct {
	statusFlags uint16
}

// build は EOF_Packet のペイロードを構築する
func (p *eofPacket) build() []byte {
	buf := []byte{0xFE}

	// warnings: 0
	buf = append(buf, 0x00, 0x00)

	// status_flags
	flags := make([]byte, 2)
	putUint16(flags, p.statusFlags)
	buf = append(buf, flags...)

	return buf
}
