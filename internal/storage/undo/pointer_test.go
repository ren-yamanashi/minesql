package undo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestIsNull(t *testing.T) {
	t.Run("NullPointer は true を返す", func(t *testing.T) {
		// GIVEN
		p := NullPointer

		// WHEN
		result := p.IsNull()

		// THEN
		assert.True(t, result)
	})

	t.Run("有効な Pointer は false を返す", func(t *testing.T) {
		// GIVEN
		p := Pointer{PageNumber: 1, Offset: 10}

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

	t.Run("PageNumber だけ一致しても false を返す", func(t *testing.T) {
		// GIVEN
		p := Pointer{PageNumber: 0xFFFF, Offset: 0}

		// WHEN
		result := p.IsNull()

		// THEN
		assert.False(t, result)
	})
}

func TestEncode(t *testing.T) {
	t.Run("4 バイトのバイト列にエンコードされる", func(t *testing.T) {
		// GIVEN
		p := Pointer{PageNumber: 3, Offset: 64}

		// WHEN
		buf := p.Encode()

		// THEN
		assert.Len(t, buf, PointerSize)
	})

	t.Run("NullPointer をエンコードできる", func(t *testing.T) {
		// GIVEN
		p := NullPointer

		// WHEN
		buf := p.Encode()

		// THEN
		assert.Len(t, buf, PointerSize)
		assert.Equal(t, []byte{0xFF, 0xFF, 0xFF, 0xFF}, buf)
	})

	t.Run("ゼロ値をエンコードできる", func(t *testing.T) {
		// GIVEN
		p := Pointer{}

		// WHEN
		buf := p.Encode()

		// THEN
		assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00}, buf)
	})
}

func TestDecodePointer(t *testing.T) {
	t.Run("Encode した結果を DecodePointer でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := Pointer{PageNumber: 3, Offset: 64}
		buf := original.Encode()

		// WHEN
		decoded, err := DecodePointer(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original, decoded)
	})

	t.Run("NullPointer のラウンドトリップ", func(t *testing.T) {
		// GIVEN
		buf := NullPointer.Encode()

		// WHEN
		decoded, err := DecodePointer(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, NullPointer, decoded)
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
		assert.Equal(t, page.PageNumber(0), decoded.PageNumber)
		assert.Equal(t, uint16(0), decoded.Offset)
	})

	t.Run("最大値のラウンドトリップ", func(t *testing.T) {
		// GIVEN
		original := Pointer{PageNumber: page.PageNumber(0xFFFF), Offset: 0xFFFF}
		buf := original.Encode()

		// WHEN
		decoded, err := DecodePointer(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original, decoded)
	})

	t.Run("データが PointerSize 未満の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		buf := []byte{0x00, 0x01, 0x02}

		// WHEN
		_, err := DecodePointer(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidUndoPtrData)
	})

	t.Run("空のデータの場合エラーを返す", func(t *testing.T) {
		// GIVEN
		buf := []byte{}

		// WHEN
		_, err := DecodePointer(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidUndoPtrData)
	})

	t.Run("nil の場合エラーを返す", func(t *testing.T) {
		// WHEN
		_, err := DecodePointer(nil)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidUndoPtrData)
	})

	t.Run("PointerSize より長いデータでも先頭 4 バイトからデコードできる", func(t *testing.T) {
		// GIVEN
		original := Pointer{PageNumber: 5, Offset: 128}
		buf := append(original.Encode(), 0xFF, 0xFF) // 余分なデータ

		// WHEN
		decoded, err := DecodePointer(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original, decoded)
	})
}
