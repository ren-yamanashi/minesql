package undo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewAllocateFileIdUndoRecord(t *testing.T) {
	t.Run("FileId を保持する", func(t *testing.T) {
		// GIVEN
		fileId := page.FileId(7)

		// WHEN
		record := NewAllocateFileIdUndoRecord(fileId)

		// THEN
		assert.Equal(t, fileId, record.FileId())
	})
}

func TestAllocateFileIdUndoRecordSerialize(t *testing.T) {
	t.Run("Serialize の出力を Deserialize でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := NewAllocateFileIdUndoRecord(page.FileId(0xCAFEBABE))

		// WHEN
		buf := original.Serialize()
		got, err := DeserializeAllocateFileIdUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original.FileId(), got.FileId())
	})

	t.Run("Serialize の長さは 4 バイトである", func(t *testing.T) {
		// GIVEN
		record := NewAllocateFileIdUndoRecord(page.FileId(1))

		// WHEN
		buf := record.Serialize()

		// THEN
		assert.Len(t, buf, allocateFileIdSize)
	})

	t.Run("FileId 0 もラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := NewAllocateFileIdUndoRecord(page.FileId(0))

		// WHEN
		buf := original.Serialize()
		got, err := DeserializeAllocateFileIdUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.FileId(0), got.FileId())
	})

	t.Run("FileId 最大値もラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := NewAllocateFileIdUndoRecord(page.MaxFileId)

		// WHEN
		buf := original.Serialize()
		got, err := DeserializeAllocateFileIdUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.MaxFileId, got.FileId())
	})
}

func TestDeserializeAllocateFileIdUndoRecord(t *testing.T) {
	t.Run("バイト長が 4 バイトでない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		buf := []byte{0x01, 0x02, 0x03}

		// WHEN
		_, err := DeserializeAllocateFileIdUndoRecord(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidAllocateFileIdUndoRecord)
	})
}
