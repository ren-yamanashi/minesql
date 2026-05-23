package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPage(t *testing.T) {
	t.Run("4KB のデータから Page を生成できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, Size)
		data[0] = 0x01
		data[HeaderSize] = 0x02

		// WHEN
		p, err := NewPage(data)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, data[:HeaderSize], p.Header)
		assert.Equal(t, data[HeaderSize:], p.Body)
	})

	t.Run("Header はデータの先頭 4 バイトを参照する", func(t *testing.T) {
		// GIVEN
		data := make([]byte, Size)
		data[0] = 0xAA
		data[1] = 0xBB
		data[2] = 0xCC
		data[3] = 0xDD

		// WHEN
		p, err := NewPage(data)

		// THEN
		assert.NoError(t, err)
		expected := []byte{0xAA, 0xBB, 0xCC, 0xDD}
		assert.Equal(t, expected, p.Header)
	})

	t.Run("Body はヘッダー以降のデータを参照する", func(t *testing.T) {
		// GIVEN
		data := make([]byte, Size)
		data[HeaderSize] = 0xFF

		// WHEN
		p, err := NewPage(data)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Size-HeaderSize, len(p.Body))
		assert.Equal(t, byte(0xFF), p.Body[0])
	})

	t.Run("データサイズが 4KB 未満の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, Size-1)

		// WHEN
		p, err := NewPage(data)

		// THEN
		assert.ErrorIs(t, err, errInvalidDataSize)
		assert.Nil(t, p)
	})

	t.Run("データサイズが 4KB 超過の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, Size+1)

		// WHEN
		p, err := NewPage(data)

		// THEN
		assert.ErrorIs(t, err, errInvalidDataSize)
		assert.Nil(t, p)
	})

	t.Run("空のデータの場合エラーを返す", func(t *testing.T) {
		// GIVEN
		data := []byte{}

		// WHEN
		p, err := NewPage(data)

		// THEN
		assert.ErrorIs(t, err, errInvalidDataSize)
		assert.Nil(t, p)
	})

	t.Run("nil の場合エラーを返す", func(t *testing.T) {
		// WHEN
		p, err := NewPage(nil)

		// THEN
		assert.ErrorIs(t, err, errInvalidDataSize)
		assert.Nil(t, p)
	})
}

func TestPageToBytes(t *testing.T) {
	t.Run("Header と Body を結合したバイト列を返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, Size)
		data[0] = 0xAA
		data[HeaderSize] = 0xBB
		p, _ := NewPage(data)

		// WHEN
		result := p.ToBytes()

		// THEN
		assert.Equal(t, Size, len(result))
		assert.Equal(t, byte(0xAA), result[0])
		assert.Equal(t, byte(0xBB), result[HeaderSize])
	})

	t.Run("NewPage で生成した Page を ToBytes で変換すると元のデータと一致する", func(t *testing.T) {
		// GIVEN
		data := make([]byte, Size)
		for i := range data {
			data[i] = byte(i % 256)
		}
		p, _ := NewPage(data)

		// WHEN
		result := p.ToBytes()

		// THEN
		assert.Equal(t, data, result)
	})

	t.Run("ToBytes は元のデータと同じメモリ領域を返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, Size)
		p, _ := NewPage(data)

		// WHEN
		result := p.ToBytes()
		result[0] = 0xFF
		result[HeaderSize] = 0xEE

		// THEN
		assert.Equal(t, byte(0xFF), p.Header[0])
		assert.Equal(t, byte(0xEE), p.Body[0])
	})

	t.Run("Header への書き込みが ToBytes の結果に反映される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, Size)
		p, _ := NewPage(data)

		// WHEN
		p.Header[0] = 0xFF
		result := p.ToBytes()

		// THEN
		assert.Equal(t, byte(0xFF), result[0])
	})

	t.Run("Body への書き込みが ToBytes の結果に反映される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, Size)
		p, _ := NewPage(data)

		// WHEN
		p.Body[0] = 0xEE
		result := p.ToBytes()

		// THEN
		assert.Equal(t, byte(0xEE), result[HeaderSize])
	})
}

func TestCopy(t *testing.T) {
	t.Run("コピーされた Page は元の Page と同じデータを持つ", func(t *testing.T) {
		// GIVEN
		data := make([]byte, Size)
		data[0] = 0xAA
		data[HeaderSize] = 0xBB
		original, _ := NewPage(data)

		// WHEN
		copied := Copy(*original)

		// THEN
		assert.Equal(t, original.ToBytes(), copied.ToBytes())
	})

	t.Run("コピーされた Page は元の Page と異なるメモリ領域を持つ", func(t *testing.T) {
		// GIVEN
		data := make([]byte, Size)
		data[0] = 0x01
		original, _ := NewPage(data)

		// WHEN
		copied := Copy(*original)
		copied.Header[0] = 0xFF

		// THEN
		assert.Equal(t, byte(0x01), original.Header[0])
		assert.Equal(t, byte(0xFF), copied.Header[0])
	})

	t.Run("Header が nil の場合はゼロ値の Page を返す", func(t *testing.T) {
		// GIVEN
		pg := Page{}

		// WHEN
		copied := Copy(pg)

		// THEN
		assert.Nil(t, copied.Header)
		assert.Nil(t, copied.Body)
	})
}

func TestCheckPageSize(t *testing.T) {
	t.Run("PageSize と一致する場合 nil を返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, Size)

		// WHEN
		err := CheckPageSize(data)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("PageSize と一致しない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 100)

		// WHEN
		err := CheckPageSize(data)

		// THEN
		assert.ErrorIs(t, err, errInvalidDataSize)
	})

	t.Run("nil の場合エラーを返す", func(t *testing.T) {
		// WHEN
		err := CheckPageSize(nil)

		// THEN
		assert.ErrorIs(t, err, errInvalidDataSize)
	})
}
