package undo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewCreateBTreeUndoRecord(t *testing.T) {
	t.Run("MetaPageId を保持する", func(t *testing.T) {
		// GIVEN
		metaPageId := page.NewId(page.FileId(7), page.PageNumber(42))

		// WHEN
		record := NewCreateBTreeUndoRecord(metaPageId)

		// THEN
		assert.Equal(t, metaPageId, record.MetaPageId())
	})
}

func TestCreateBTreeUndoRecordSerialize(t *testing.T) {
	t.Run("Serialize の出力を Deserialize でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := NewCreateBTreeUndoRecord(page.NewId(page.FileId(7), page.PageNumber(42)))

		// WHEN
		buf := original.Serialize()
		got, err := DeserializeCreateBTreeUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original.MetaPageId(), got.MetaPageId())
	})

	t.Run("Serialize の長さは page.IdSize と一致する", func(t *testing.T) {
		// GIVEN
		record := NewCreateBTreeUndoRecord(page.NewId(page.FileId(1), page.PageNumber(2)))

		// WHEN
		buf := record.Serialize()

		// THEN
		assert.Len(t, buf, page.IdSize)
	})

	t.Run("無効値 PageId もラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := NewCreateBTreeUndoRecord(page.InvalidId())

		// WHEN
		buf := original.Serialize()
		got, err := DeserializeCreateBTreeUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.True(t, got.MetaPageId().IsInvalid())
	})
}

func TestDeserializeCreateBTreeUndoRecord(t *testing.T) {
	t.Run("バイト長が page.IdSize より短い場合 ErrInvalidCreateBTreeUndoRecord", func(t *testing.T) {
		// GIVEN
		buf := make([]byte, page.IdSize-1)

		// WHEN
		_, err := DeserializeCreateBTreeUndoRecord(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidCreateBTreeUndoRecord)
	})

	t.Run("バイト長が page.IdSize より長い場合 ErrInvalidCreateBTreeUndoRecord", func(t *testing.T) {
		// GIVEN
		buf := make([]byte, page.IdSize+1)

		// WHEN
		_, err := DeserializeCreateBTreeUndoRecord(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidCreateBTreeUndoRecord)
	})
}
