package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPage(t *testing.T) {
	t.Run("4KB のデータから Page を生成できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize)
		data[0] = 0x01
		data[PageHeaderSize] = 0x02

		// WHEN
		p, err := NewPage(data)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, data[:PageHeaderSize], p.Header)
		assert.Equal(t, data[PageHeaderSize:], p.Body)
	})

	t.Run("Header はデータの先頭 4 バイトを参照する", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize)
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
		data := make([]byte, PageSize)
		data[PageHeaderSize] = 0xFF

		// WHEN
		p, err := NewPage(data)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, PageSize-PageHeaderSize, len(p.Body))
		assert.Equal(t, byte(0xFF), p.Body[0])
	})

	t.Run("データサイズが 4KB 未満の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize-1)

		// WHEN
		p, err := NewPage(data)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidDataSize)
		assert.Nil(t, p)
	})

	t.Run("データサイズが 4KB 超過の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize+1)

		// WHEN
		p, err := NewPage(data)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidDataSize)
		assert.Nil(t, p)
	})

	t.Run("空のデータの場合エラーを返す", func(t *testing.T) {
		// GIVEN
		data := []byte{}

		// WHEN
		p, err := NewPage(data)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidDataSize)
		assert.Nil(t, p)
	})
}

func TestPageToBytes(t *testing.T) {
	t.Run("Header と Body を結合したバイト列を返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize)
		data[0] = 0xAA
		data[PageHeaderSize] = 0xBB
		p, _ := NewPage(data)

		// WHEN
		result := p.ToBytes()

		// THEN
		assert.Equal(t, PageSize, len(result))
		assert.Equal(t, byte(0xAA), result[0])
		assert.Equal(t, byte(0xBB), result[PageHeaderSize])
	})

	t.Run("NewPage で生成した Page を ToBytes で変換すると元のデータと一致する", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize)
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
		data := make([]byte, PageSize)
		p, _ := NewPage(data)

		// WHEN
		result := p.ToBytes()
		result[0] = 0xFF
		result[PageHeaderSize] = 0xEE

		// THEN
		assert.Equal(t, byte(0xFF), p.Header[0])
		assert.Equal(t, byte(0xEE), p.Body[0])
	})

	t.Run("Header への書き込みが ToBytes の結果に反映される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize)
		p, _ := NewPage(data)

		// WHEN
		p.Header[0] = 0xFF
		result := p.ToBytes()

		// THEN
		assert.Equal(t, byte(0xFF), result[0])
	})

	t.Run("Body への書き込みが ToBytes の結果に反映される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize)
		p, _ := NewPage(data)

		// WHEN
		p.Body[0] = 0xEE
		result := p.ToBytes()

		// THEN
		assert.Equal(t, byte(0xEE), result[PageHeaderSize])
	})
}

func TestCheckPageSize(t *testing.T) {
	t.Run("PageSize と一致する場合 nil を返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize)

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
		assert.ErrorIs(t, err, ErrInvalidDataSize)
	})
}
