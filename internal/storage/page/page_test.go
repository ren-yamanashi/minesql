package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPage(t *testing.T) {
	t.Run("16KB のデータから Page を生成できる", func(t *testing.T) {
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

	t.Run("データサイズが 16KB 未満の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize-1)

		// WHEN
		p, err := NewPage(data)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidDataSize)
		assert.Nil(t, p)
	})

	t.Run("データサイズが 16KB 超過の場合エラーを返す", func(t *testing.T) {
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
