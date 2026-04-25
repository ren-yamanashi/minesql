package server

// エラーコード定数
const (
	erAccessDenied uint16 = 1045
	erParseError   uint16 = 1064
	erUnknownError uint16 = 1105
)

// SQL State 定数
const (
	sqlStateAuthError    = "28000" // 認証失敗
	sqlStateSyntaxError  = "42000" // 構文エラー
	sqlStateGeneralError = "HY000" // 汎用エラー
)

// errPacket は ERR_Packet を表す
//
// 構造:
//   - 0xFF (ヘッダー)
//   - error_code (2 バイト LittleEndian)
//   - sql_state_marker: '#'
//   - sql_state (5 バイト固定文字列)
//   - error_message (残りの文字列)
type errPacket struct {
	errorCode uint16
	sqlState  string
	message   string
}

// build は ERR_Packet のペイロードを構築する
func (p *errPacket) build() []byte {
	buf := []byte{0xFF}

	code := make([]byte, 2)
	putUint16(code, p.errorCode)
	buf = append(buf, code...)

	buf = append(buf, '#')
	buf = append(buf, []byte(p.sqlState)...)
	buf = append(buf, []byte(p.message)...)
	return buf
}
