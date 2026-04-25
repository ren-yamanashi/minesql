package server

// columnDefPacket は結果セットのカラムメタデータであり、Column Definition パケットを表す
type columnDefPacket struct {
	tableName string
	name      string
}

// build は Column Definition パケットのペイロードを構築する
func (c *columnDefPacket) build() []byte {
	var buf []byte

	// catalog: "def"
	buf = putLenEncString(buf, "def")

	// schema: "" (空文字列)
	buf = putLenEncString(buf, "")

	// table
	buf = putLenEncString(buf, c.tableName)

	// org_table
	buf = putLenEncString(buf, c.tableName)

	// name
	buf = putLenEncString(buf, c.name)

	// org_name
	buf = putLenEncString(buf, c.name)

	// fixed_fields_length: 0x0c
	buf = putLenEncInt(buf, 0x0c)

	// character_set: utf8mb4_general_ci (2 バイト)
	cs := make([]byte, 2)
	putUint16(cs, charsetUTF8MB4)
	buf = append(buf, cs...)

	// column_length: 255 (4 バイト、VARCHAR のデフォルト)
	cl := make([]byte, 4)
	putUint32(cl, 255)
	buf = append(buf, cl...)

	// column_type: MYSQL_TYPE_VAR_STRING (1 バイト)
	buf = append(buf, 0xFD)

	// flags: 0x0000 (2 バイト)
	buf = append(buf, 0x00, 0x00)

	// decimals: 0x00 (1 バイト)
	buf = append(buf, 0x00)

	// filler: 0x0000 (2 バイト)
	buf = append(buf, 0x00, 0x00)

	return buf
}
