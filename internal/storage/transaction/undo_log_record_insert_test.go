package transaction

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertLogRecord_Undo(t *testing.T) {
	t.Run("Insert した行が物理削除される", func(t *testing.T) {
		// GIVEN
		table, bp := setupTestTableForUndo(t, nil)

		record := [][]byte{[]byte("a"), []byte("John")}
		err := table.Insert(bp, record)
		assert.NoError(t, err)

		undoRecord := UndoInsertRecord{table: table, Record: record}

		// WHEN
		err = undoRecord.Undo(bp)

		// THEN: レコードが物理削除されている (B+Tree にも残らない)
		assert.NoError(t, err)
		tree := btree.NewBTree(table.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)
		_, ok := iter.Get()
		assert.False(t, ok)
	})

	t.Run("Insert した行のユニークインデックスも物理削除される", func(t *testing.T) {
		// GIVEN
		uniqueIndex := access.NewUniqueIndexAccessMethod("idx_name", "name", page.PageId{}, 1, 1)
		table, bp := setupTestTableForUndo(t, []*access.UniqueIndexAccessMethod{uniqueIndex})

		record := [][]byte{[]byte("a"), []byte("John")}
		err := table.Insert(bp, record)
		assert.NoError(t, err)

		undoRecord := UndoInsertRecord{table: table, Record: record}

		// WHEN
		err = undoRecord.Undo(bp)

		// THEN: ユニークインデックスからも物理削除されている
		assert.NoError(t, err)
		keys := collectActiveUniqueIndexKeys(t, table.UniqueIndexes[0], bp)
		assert.Equal(t, 0, len(keys))
	})
}
