package undo

import (
	"minesql/internal/access"
	"minesql/internal/engine"
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateInplaceLogRecord_Undo(t *testing.T) {
	t.Run("UpdateInplace した行が元の値に戻る", func(t *testing.T) {
		// GIVEN
		table := setupTestTable(t, nil)
		defer engine.Reset()
		bp := engine.Get().BufferPool

		prevRecord := [][]byte{[]byte("a"), []byte("John")}
		newRecord := [][]byte{[]byte("a"), []byte("Jane")}
		err := table.Insert(bp, prevRecord)
		assert.NoError(t, err)
		err = table.UpdateInplace(bp, prevRecord, newRecord)
		assert.NoError(t, err)

		undoRecord := UpdateInplaceLogRecord{
			table: table, PrevRecord: prevRecord, NewRecord: newRecord,
		}

		// WHEN
		err = undoRecord.Undo()

		// THEN: 元の値に戻っている
		assert.NoError(t, err)
		records := collectActiveRecords(t, table)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, []string{"a", "John"}, records[0])
	})

	t.Run("ユニークインデックスも元の値に戻る", func(t *testing.T) {
		// GIVEN
		uniqueIndex := access.NewUniqueIndexAccessMethod("idx_name", "name", page.PageId{}, 1)
		table := setupTestTable(t, []*access.UniqueIndexAccessMethod{uniqueIndex})
		defer engine.Reset()
		bp := engine.Get().BufferPool

		prevRecord := [][]byte{[]byte("a"), []byte("John")}
		newRecord := [][]byte{[]byte("a"), []byte("Jane")}
		err := table.Insert(bp, prevRecord)
		assert.NoError(t, err)
		err = table.UpdateInplace(bp, prevRecord, newRecord)
		assert.NoError(t, err)

		undoRecord := UpdateInplaceLogRecord{
			table: table, PrevRecord: prevRecord, NewRecord: newRecord,
		}

		// WHEN
		err = undoRecord.Undo()

		// THEN: ユニークインデックスも元の値に戻っている
		assert.NoError(t, err)
		keys := collectActiveUniqueIndexKeys(t, table.UniqueIndexes[0])
		assert.Equal(t, []string{"John"}, keys)
	})
}
