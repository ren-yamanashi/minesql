package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUndoPtrIsNull(t *testing.T) {
	t.Run("NullUndoPtr は null", func(t *testing.T) {
		// GIVEN
		ptr := NullUndoPtr

		// WHEN / THEN
		assert.True(t, ptr.IsNull())
	})

	t.Run("ゼロ値は null ではない", func(t *testing.T) {
		// GIVEN
		// UndoPtr{0, 0} は undo ファイルの最初のページの先頭を指す有効なポインタ
		ptr := UndoPtr{}

		// WHEN / THEN
		assert.False(t, ptr.IsNull())
	})

	t.Run("通常の値は null ではない", func(t *testing.T) {
		// GIVEN
		ptr := UndoPtr{PageNumber: 1, Offset: 10}

		// WHEN / THEN
		assert.False(t, ptr.IsNull())
	})
}

func TestUndoPtrEncodeAndDecode(t *testing.T) {
	t.Run("エンコードしたバイト列をデコードすると元の値に戻る", func(t *testing.T) {
		// GIVEN
		ptr := UndoPtr{PageNumber: 3, Offset: 42}

		// WHEN
		encoded := ptr.Encode()
		decoded, err := DecodeUndoPtr(encoded)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, ptr, decoded)
		assert.Equal(t, UndoPtrSize, len(encoded))
	})

	t.Run("NullUndoPtr のラウンドトリップ", func(t *testing.T) {
		// GIVEN
		ptr := NullUndoPtr

		// WHEN
		encoded := ptr.Encode()
		decoded, err := DecodeUndoPtr(encoded)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, ptr, decoded)
		assert.True(t, decoded.IsNull())
	})

	t.Run("4 バイト未満のデータを渡すとエラーを返す", func(t *testing.T) {
		// GIVEN
		shortData := []byte{0x00, 0x01}

		// WHEN
		_, err := DecodeUndoPtr(shortData)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidUndoPtrData)
	})

	t.Run("最大値のラウンドトリップ", func(t *testing.T) {
		// GIVEN
		ptr := UndoPtr{PageNumber: 0xFFFF, Offset: 0xFFFF}

		// WHEN
		encoded := ptr.Encode()
		decoded, err := DecodeUndoPtr(encoded)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, ptr, decoded)
	})
}
