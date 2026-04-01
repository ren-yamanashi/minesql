package transaction

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUndoDeleteRecord_Undo(t *testing.T) {
	t.Run("SoftDelete した行が復元される", func(t *testing.T) {
		// GIVEN
		table, bp := setupTestTableForUndo(t, nil)

		record := [][]byte{[]byte("a"), []byte("John")}
		err := table.Insert(bp, record)
		assert.NoError(t, err)
		err = table.SoftDelete(bp, record)
		assert.NoError(t, err)

		undoRecord := UndoDeleteRecord{table: table, Record: record}

		// WHEN
		err = undoRecord.Undo(bp)

		// THEN: レコードが active に戻っている
		assert.NoError(t, err)
		records := collectActiveRecords(t, table, bp)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, []string{"a", "John"}, records[0])
	})

	t.Run("SoftDelete した行のユニークインデックスも復元される", func(t *testing.T) {
		// GIVEN
		uniqueIndex := access.NewUniqueIndexAccessMethod("idx_name", "name", page.PageId{}, 1)
		table, bp := setupTestTableForUndo(t, []*access.UniqueIndexAccessMethod{uniqueIndex})

		record := [][]byte{[]byte("a"), []byte("John")}
		err := table.Insert(bp, record)
		assert.NoError(t, err)
		err = table.SoftDelete(bp, record)
		assert.NoError(t, err)

		undoRecord := UndoDeleteRecord{table: table, Record: record}

		// WHEN
		err = undoRecord.Undo(bp)

		// THEN: ユニークインデックスも復元されている
		assert.NoError(t, err)
		keys := collectActiveUniqueIndexKeys(t, table.UniqueIndexes[0], bp)
		assert.Equal(t, []string{"John"}, keys)
	})

	t.Run("物理削除した行が再挿入される", func(t *testing.T) {
		// GIVEN
		table, bp := setupTestTableForUndo(t, nil)

		record := [][]byte{[]byte("a"), []byte("John")}
		err := table.Insert(bp, record)
		assert.NoError(t, err)
		err = table.Delete(bp, record)
		assert.NoError(t, err)

		undoRecord := UndoDeleteRecord{table: table, Record: record}

		// WHEN
		err = undoRecord.Undo(bp)

		// THEN: レコードが再挿入されている
		assert.NoError(t, err)
		records := collectActiveRecords(t, table, bp)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, []string{"a", "John"}, records[0])
	})
}
