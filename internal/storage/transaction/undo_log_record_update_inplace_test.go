package transaction

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateInplaceLogRecord_Undo(t *testing.T) {
	t.Run("UpdateInplace した行が元の値に戻る", func(t *testing.T) {
		// GIVEN
		table, bp := setupTestTableForUndo(t, nil)

		prevRecord := [][]byte{[]byte("a"), []byte("John")}
		newRecord := [][]byte{[]byte("a"), []byte("Jane")}
		err := table.Insert(bp, prevRecord)
		assert.NoError(t, err)
		err = table.UpdateInplace(bp, 0, nil, prevRecord, newRecord)
		assert.NoError(t, err)

		undoRecord := UndoUpdateInplaceRecord{
			table: table, PrevRecord: prevRecord, NewRecord: newRecord,
		}

		// WHEN
		err = undoRecord.Undo(bp)

		// THEN: 元の値に戻っている
		assert.NoError(t, err)
		records := collectActiveRecords(t, table, bp)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, []string{"a", "John"}, records[0])
	})

	t.Run("ユニークインデックスも元の値に戻る", func(t *testing.T) {
		// GIVEN
		uniqueIndex := access.NewUniqueIndex("idx_name", "name", page.PageId{}, 1, 1)
		table, bp := setupTestTableForUndo(t, []*access.UniqueIndex{uniqueIndex})

		prevRecord := [][]byte{[]byte("a"), []byte("John")}
		newRecord := [][]byte{[]byte("a"), []byte("Jane")}
		err := table.Insert(bp, prevRecord)
		assert.NoError(t, err)
		err = table.UpdateInplace(bp, 0, nil, prevRecord, newRecord)
		assert.NoError(t, err)

		undoRecord := UndoUpdateInplaceRecord{
			table: table, PrevRecord: prevRecord, NewRecord: newRecord,
		}

		// WHEN
		err = undoRecord.Undo(bp)

		// THEN: ユニークインデックスも元の値に戻っている
		assert.NoError(t, err)
		keys := collectActiveUniqueIndexKeys(t, table.UniqueIndexes[0], bp)
		assert.Equal(t, []string{"John"}, keys)
	})
}
