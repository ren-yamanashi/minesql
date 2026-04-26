package server

// okPacket は OK_Packet を表す
//
// 構造:
//   - ヘッダー (通常は 0x00、EOF 代替時は 0xFE)
//   - affected_rows (長さエンコード整数)
//   - last_insert_id (長さエンコード整数)
//   - status_flags (2 バイト LittleEndian)
//   - warnings (2 バイト、常に 0)
type okPacket struct {
	affectedRows uint64
	lastInsertId uint64
	statusFlags  uint16
	isEOF        bool // true の場合、ヘッダーを 0xFE にする (結果セット終了の EOF 代替)
}

// build は OK_Packet のペイロードを構築する
func (p *okPacket) build() []byte {
	header := byte(0x00)
	if p.isEOF {
		header = 0xFE
	}
	buf := []byte{header}
	buf = putLenEncInt(buf, p.affectedRows)
	buf = putLenEncInt(buf, p.lastInsertId)

	flags := make([]byte, 2)
	putUint16(flags, p.statusFlags)
	buf = append(buf, flags...)

	// warnings: 0
	buf = append(buf, 0x00, 0x00)
	return buf
}
