package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFixedLengthIntegers(t *testing.T) {
	t.Run("uint16 の書き込みと読み取りが一致する", func(t *testing.T) {
		// GIVEN
		buf := make([]byte, 2)

		// WHEN
		putUint16(buf, 0x1234)

		// THEN
		assert.Equal(t, uint16(0x1234), readUint16(buf))
		assert.Equal(t, byte(0x34), buf[0]) // LittleEndian: 下位バイトが先
		assert.Equal(t, byte(0x12), buf[1])
	})

	t.Run("uint24 の書き込みと読み取りが一致する", func(t *testing.T) {
		// GIVEN
		buf := make([]byte, 3)

		// WHEN
		putUint24(buf, 0x123456)

		// THEN
		assert.Equal(t, uint32(0x123456), readUint24(buf))
		assert.Equal(t, byte(0x56), buf[0])
		assert.Equal(t, byte(0x34), buf[1])
		assert.Equal(t, byte(0x12), buf[2])
	})

	t.Run("uint32 の書き込みと読み取りが一致する", func(t *testing.T) {
		// GIVEN
		buf := make([]byte, 4)

		// WHEN
		putUint32(buf, 0x12345678)

		// THEN
		assert.Equal(t, uint32(0x12345678), readUint32(buf))
		assert.Equal(t, byte(0x78), buf[0])
		assert.Equal(t, byte(0x56), buf[1])
		assert.Equal(t, byte(0x34), buf[2])
		assert.Equal(t, byte(0x12), buf[3])
	})
}

func TestLenEncInt(t *testing.T) {
	t.Run("1 バイト整数 (0-250)", func(t *testing.T) {
		// WHEN
		buf := putLenEncInt(nil, 100)

		// THEN
		assert.Equal(t, []byte{100}, buf)

		v, rest, err := readLenEncInt(buf)
		assert.NoError(t, err)
		assert.Equal(t, uint64(100), v)
		assert.Empty(t, rest)
	})

	t.Run("3 バイト整数 (251-65535)", func(t *testing.T) {
		// WHEN
		buf := putLenEncInt(nil, 1000)

		// THEN
		assert.Equal(t, byte(0xFC), buf[0])
		assert.Len(t, buf, 3)

		v, rest, err := readLenEncInt(buf)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1000), v)
		assert.Empty(t, rest)
	})

	t.Run("4 バイト整数 (65536-16777215)", func(t *testing.T) {
		// WHEN
		buf := putLenEncInt(nil, 100000)

		// THEN
		assert.Equal(t, byte(0xFD), buf[0])
		assert.Len(t, buf, 4)

		v, rest, err := readLenEncInt(buf)
		assert.NoError(t, err)
		assert.Equal(t, uint64(100000), v)
		assert.Empty(t, rest)
	})

	t.Run("9 バイト整数 (16777216 以上)", func(t *testing.T) {
		// WHEN
		buf := putLenEncInt(nil, 1<<24)

		// THEN
		assert.Equal(t, byte(0xFE), buf[0])
		assert.Len(t, buf, 9)

		v, rest, err := readLenEncInt(buf)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1<<24), v)
		assert.Empty(t, rest)
	})

	t.Run("0 の場合", func(t *testing.T) {
		// WHEN
		buf := putLenEncInt(nil, 0)

		// THEN
		assert.Equal(t, []byte{0}, buf)

		v, rest, err := readLenEncInt(buf)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), v)
		assert.Empty(t, rest)
	})

	t.Run("250 (1 バイトの上限)", func(t *testing.T) {
		// WHEN
		buf := putLenEncInt(nil, 250)

		// THEN
		assert.Equal(t, []byte{250}, buf)

		v, rest, err := readLenEncInt(buf)
		assert.NoError(t, err)
		assert.Equal(t, uint64(250), v)
		assert.Empty(t, rest)
	})

	t.Run("既存バッファに追記できる", func(t *testing.T) {
		// GIVEN
		buf := []byte{0x01, 0x02}

		// WHEN
		buf = putLenEncInt(buf, 42)

		// THEN
		assert.Equal(t, []byte{0x01, 0x02, 42}, buf)
	})

	t.Run("空バッファでエラーを返す", func(t *testing.T) {
		// WHEN
		_, _, err := readLenEncInt([]byte{})

		// THEN
		assert.Error(t, err)
	})

	t.Run("バッファが短い場合にエラーを返す (0xFC)", func(t *testing.T) {
		// WHEN: 0xFC は 3 バイト必要だが 2 バイトしかない
		_, _, err := readLenEncInt([]byte{0xFC, 0x01})

		// THEN
		assert.Error(t, err)
	})
}

func TestNullTermString(t *testing.T) {
	t.Run("書き込みと読み取りが一致する", func(t *testing.T) {
		// WHEN
		buf := putNullTermString(nil, "hello")

		// THEN
		assert.Equal(t, []byte{'h', 'e', 'l', 'l', 'o', 0x00}, buf)

		s, rest := readNullTermString(buf)
		assert.Equal(t, "hello", s)
		assert.Empty(t, rest)
	})

	t.Run("空文字列", func(t *testing.T) {
		// WHEN
		buf := putNullTermString(nil, "")

		// THEN
		assert.Equal(t, []byte{0x00}, buf)

		s, rest := readNullTermString(buf)
		assert.Equal(t, "", s)
		assert.Empty(t, rest)
	})

	t.Run("後続データがある場合", func(t *testing.T) {
		// GIVEN
		buf := putNullTermString(nil, "abc")
		buf = append(buf, 0xFF) // 後続データ

		// WHEN
		s, rest := readNullTermString(buf)

		// THEN
		assert.Equal(t, "abc", s)
		assert.Equal(t, []byte{0xFF}, rest)
	})
}

func TestLenEncString(t *testing.T) {
	t.Run("書き込みと読み取りが一致する", func(t *testing.T) {
		// WHEN
		buf := putLenEncString(nil, "hello")

		// THEN
		assert.Equal(t, byte(5), buf[0]) // 長さ
		assert.Equal(t, "hello", string(buf[1:]))

		s, rest, err := readLenEncString(buf)
		assert.NoError(t, err)
		assert.Equal(t, "hello", s)
		assert.Empty(t, rest)
	})

	t.Run("空文字列", func(t *testing.T) {
		// WHEN
		buf := putLenEncString(nil, "")

		// THEN
		assert.Equal(t, []byte{0}, buf)

		s, rest, err := readLenEncString(buf)
		assert.NoError(t, err)
		assert.Equal(t, "", s)
		assert.Empty(t, rest)
	})
}

func TestFixedString(t *testing.T) {
	t.Run("指定バイト数を読み取る", func(t *testing.T) {
		// GIVEN
		buf := []byte("helloworld")

		// WHEN
		s, rest := readFixedString(buf, 5)

		// THEN
		assert.Equal(t, "hello", s)
		assert.Equal(t, []byte("world"), rest)
	})
}

func TestRestOfPacketString(t *testing.T) {
	t.Run("残り全体を文字列として返す", func(t *testing.T) {
		// GIVEN
		buf := []byte("remaining data")

		// WHEN
		s := readRestOfPacketString(buf)

		// THEN
		assert.Equal(t, "remaining data", s)
	})
}
