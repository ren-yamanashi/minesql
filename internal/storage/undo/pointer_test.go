package undo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewPointer(t *testing.T) {
	t.Run("指定した PageNumber と offset で Pointer を作成できる", func(t *testing.T) {
		// GIVEN
		pageNum := page.PageNumber(3)
		offset := uint16(64)

		// WHEN
		p := NewPointer(pageNum, offset)

		// THEN
		assert.Equal(t, page.PageNumber(3), p.pageNumber)
		assert.Equal(t, uint16(64), p.offset)
	})

	t.Run("ゼロ値で Pointer を作成できる", func(t *testing.T) {
		// GIVEN / WHEN
		p := NewPointer(0, 0)

		// THEN
		assert.Equal(t, page.PageNumber(0), p.pageNumber)
		assert.Equal(t, uint16(0), p.offset)
	})

	t.Run("最大値で Pointer を作成できる", func(t *testing.T) {
		// GIVEN / WHEN
		p := NewPointer(page.PageNumber(0xFFFFFFFF), 0xFFFF)

		// THEN
		assert.Equal(t, page.PageNumber(0xFFFFFFFF), p.pageNumber)
		assert.Equal(t, uint16(0xFFFF), p.offset)
	})
}

func TestPointerEncode(t *testing.T) {
	t.Run("6 バイトのバイト列にエンコードされる", func(t *testing.T) {
		// GIVEN
		p := Pointer{pageNumber: 3, offset: 64}

		// WHEN
		buf := p.Encode()

		// THEN
		assert.Len(t, buf, PointerSize)
	})

	t.Run("NullPointer をエンコードできる", func(t *testing.T) {
		// GIVEN
		p := NullPointer()

		// WHEN
		buf := p.Encode()

		// THEN
		assert.Len(t, buf, PointerSize)
		assert.Equal(t, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, buf)
	})

	t.Run("ゼロ値をエンコードできる", func(t *testing.T) {
		// GIVEN
		p := Pointer{}

		// WHEN
		buf := p.Encode()

		// THEN
		assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, buf)
	})
}

func TestPointerIsNull(t *testing.T) {
	t.Run("NullPointer は true を返す", func(t *testing.T) {
		// GIVEN
		p := NullPointer()

		// WHEN
		result := p.IsNull()

		// THEN
		assert.True(t, result)
	})

	t.Run("有効な Pointer は false を返す", func(t *testing.T) {
		// GIVEN
		p := Pointer{pageNumber: 1, offset: 10}

		// WHEN
		result := p.IsNull()

		// THEN
		assert.False(t, result)
	})

	t.Run("ゼロ値の Pointer は false を返す", func(t *testing.T) {
		// GIVEN
		p := Pointer{}

		// WHEN
		result := p.IsNull()

		// THEN
		assert.False(t, result)
	})

	t.Run("pageNumber だけ一致しても false を返す", func(t *testing.T) {
		// GIVEN
		p := Pointer{pageNumber: 0xFFFFFFFF, offset: 0}

		// WHEN
		result := p.IsNull()

		// THEN
		assert.False(t, result)
	})
}

func TestDecodePointer(t *testing.T) {
	t.Run("Encode した結果を DecodePointer でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := Pointer{pageNumber: 3, offset: 64}
		buf := original.Encode()

		// WHEN
		decoded, err := DecodePointer(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original, decoded)
	})

	t.Run("NullPointer のラウンドトリップ", func(t *testing.T) {
		// GIVEN
		buf := NullPointer().Encode()

		// WHEN
		decoded, err := DecodePointer(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, NullPointer(), decoded)
		assert.True(t, decoded.IsNull())
	})

	t.Run("ゼロ値のラウンドトリップ", func(t *testing.T) {
		// GIVEN
		original := Pointer{}
		buf := original.Encode()

		// WHEN
		decoded, err := DecodePointer(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.PageNumber(0), decoded.pageNumber)
		assert.Equal(t, uint16(0), decoded.offset)
	})

	t.Run("最大値のラウンドトリップ", func(t *testing.T) {
		// GIVEN
		original := Pointer{pageNumber: page.PageNumber(0xFFFFFFFF), offset: 0xFFFF}
		buf := original.Encode()

		// WHEN
		decoded, err := DecodePointer(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original, decoded)
	})

	t.Run("データが PointerSize 未満の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		buf := []byte{0x00, 0x01, 0x02, 0x03, 0x04}

		// WHEN
		_, err := DecodePointer(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidPointerData)
	})

	t.Run("空のデータの場合エラーを返す", func(t *testing.T) {
		// GIVEN
		buf := []byte{}

		// WHEN
		_, err := DecodePointer(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidPointerData)
	})

	t.Run("nil の場合エラーを返す", func(t *testing.T) {
		// WHEN
		_, err := DecodePointer(nil)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidPointerData)
	})

	t.Run("PointerSize より長いデータでも先頭 6 バイトからデコードできる", func(t *testing.T) {
		// GIVEN
		original := Pointer{pageNumber: 5, offset: 128}
		buf := append(original.Encode(), 0xFF, 0xFF) // 余分なデータ

		// WHEN
		decoded, err := DecodePointer(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original, decoded)
	})
}
