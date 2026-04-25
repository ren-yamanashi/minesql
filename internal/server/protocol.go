package server

import (
	"encoding/binary"
	"fmt"
)

// Capability Flags
const (
	clientConnectWithDB    uint32 = 0x00000008 // 接続時にデータベース名を指定できる
	clientProtocol41       uint32 = 0x00000200 // 4.1 プロトコルを使用する
	clientTransactions     uint32 = 0x00002000 // トランザクション状態をステータスフラグで通知する
	clientSecureConnection uint32 = 0x00008000 // 4.1 認証プロトコルを使用する
	clientPluginAuth       uint32 = 0x00080000 // 認証プラグインのネゴシエーションをサポートする
	clientDeprecateEOF     uint32 = 0x01000000 // EOF_Packet の代わりに OK_Packet を使用する
)

// serverCapability はサーバーがサポートする Capability Flags の組み合わせ
var serverCapability = clientProtocol41 |
	clientSecureConnection |
	clientPluginAuth |
	clientDeprecateEOF |
	clientTransactions |
	clientConnectWithDB

// Server Status Flags
const (
	serverStatusInTrans    uint16 = 0x0001
	serverStatusAutocommit uint16 = 0x0002
)

// --- 固定長整数 (LittleEndian) ---

func putUint16(buf []byte, v uint16) {
	binary.LittleEndian.PutUint16(buf, v)
}

func putUint24(buf []byte, v uint32) {
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
}

func putUint32(buf []byte, v uint32) {
	binary.LittleEndian.PutUint32(buf, v)
}

func readUint16(buf []byte) uint16 {
	return binary.LittleEndian.Uint16(buf)
}

func readUint24(buf []byte) uint32 {
	return uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16
}

func readUint32(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf)
}

// --- 長さエンコード整数 ---

// putLenEncInt は長さエンコード整数を buf に追記して返す
func putLenEncInt(buf []byte, v uint64) []byte {
	switch {
	case v < 251:
		return append(buf, byte(v))
	case v < 1<<16:
		return append(buf, 0xFC, byte(v), byte(v>>8))
	case v < 1<<24:
		return append(buf, 0xFD, byte(v), byte(v>>8), byte(v>>16))
	default:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, v)
		return append(append(buf, 0xFE), b...)
	}
}

// readLenEncInt は先頭から長さエンコード整数を読み取り、値と残りのスライスを返す
func readLenEncInt(buf []byte) (uint64, []byte, error) {
	if len(buf) == 0 {
		return 0, nil, fmt.Errorf("empty buffer for length-encoded integer")
	}
	first := buf[0]
	switch {
	case first < 0xFB:
		return uint64(first), buf[1:], nil
	case first == 0xFC:
		if len(buf) < 3 {
			return 0, nil, fmt.Errorf("buffer too short for 2-byte length-encoded integer: need 3, got %d", len(buf))
		}
		v := uint64(binary.LittleEndian.Uint16(buf[1:3]))
		return v, buf[3:], nil
	case first == 0xFD:
		if len(buf) < 4 {
			return 0, nil, fmt.Errorf("buffer too short for 3-byte length-encoded integer: need 4, got %d", len(buf))
		}
		v := uint64(buf[1]) | uint64(buf[2])<<8 | uint64(buf[3])<<16
		return v, buf[4:], nil
	default: // 0xFE
		if len(buf) < 9 {
			return 0, nil, fmt.Errorf("buffer too short for 8-byte length-encoded integer: need 9, got %d", len(buf))
		}
		v := binary.LittleEndian.Uint64(buf[1:9])
		return v, buf[9:], nil
	}
}

// --- 文字列型 ---

// putNullTermString は NULL 終端文字列を buf に追記して返す
func putNullTermString(buf []byte, s string) []byte {
	buf = append(buf, []byte(s)...)
	return append(buf, 0x00)
}

// readNullTermString は先頭から NULL 終端文字列を読み取り、文字列と残りのスライスを返す
func readNullTermString(buf []byte) (string, []byte) {
	for i, b := range buf {
		if b == 0x00 {
			return string(buf[:i]), buf[i+1:]
		}
	}
	return string(buf), nil
}

// putLenEncString は長さ指定付き文字列を buf に追記して返す
func putLenEncString(buf []byte, s string) []byte {
	buf = putLenEncInt(buf, uint64(len(s)))
	return append(buf, []byte(s)...)
}

// readLenEncString は先頭から長さ指定付き文字列を読み取り、文字列と残りのスライスを返す
func readLenEncString(buf []byte) (string, []byte, error) {
	length, rest, err := readLenEncInt(buf)
	if err != nil {
		return "", nil, err
	}
	if uint64(len(rest)) < length {
		return "", nil, fmt.Errorf("buffer too short for length-encoded string: need %d, got %d", length, len(rest))
	}
	s := string(rest[:length])
	return s, rest[length:], nil
}

// readFixedString は先頭から n バイトの固定長文字列を読み取り、文字列と残りのスライスを返す
func readFixedString(buf []byte, n int) (string, []byte) {
	return string(buf[:n]), buf[n:]
}

// readRestOfPacketString はバッファの残り全体を文字列として返す
func readRestOfPacketString(buf []byte) string {
	return string(buf)
}
