package access

import (
	"minesql/internal/storage/btree"
	"minesql/internal/storage/lock"
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUndoInsertRecord_Undo(t *testing.T) {
	t.Run("Insert した行が物理削除される", func(t *testing.T) {
		// GIVEN
		table, bp := setupTestTableForUndo(t, nil)

		record := [][]byte{[]byte("a"), []byte("John")}
		err := table.Insert(bp, 0, lock.NewManager(5000), record)
		assert.NoError(t, err)

		undoRecord := NewUndoInsertRecord(table, record)

		// WHEN
		err = undoRecord.Undo(bp, 0, lock.NewManager(5000))

		// THEN: レコードが物理削除されている (B+Tree にも残らない)
		assert.NoError(t, err)
		tree := btree.NewBTree(table.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Get(bp)
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("Insert した行のユニークインデックスも物理削除される", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewUniqueIndex("idx_name", "name", page.PageId{}, 1, 1)
		table, bp := setupTestTableForUndo(t, []*UniqueIndex{uniqueIndex})

		record := [][]byte{[]byte("a"), []byte("John")}
		err := table.Insert(bp, 0, lock.NewManager(5000), record)
		assert.NoError(t, err)

		undoRecord := NewUndoInsertRecord(table, record)

		// WHEN
		err = undoRecord.Undo(bp, 0, lock.NewManager(5000))

		// THEN: ユニークインデックスからも物理削除されている
		assert.NoError(t, err)
		keys := collectUndoActiveUniqueIndexKeys(t, table.UniqueIndexes[0], bp)
		assert.Equal(t, 0, len(keys))
	})
}

func TestUndoInsertRecord_Serialize(t *testing.T) {
	t.Run("シリアライズしてデシリアライズすると元のデータが復元される", func(t *testing.T) {
		// GIVEN
		table, _ := setupTestTableForUndo(t, nil)
		record := NewUndoInsertRecord(table, [][]byte{[]byte("a"), []byte("John")})

		// WHEN
		buf := record.Serialize(1, 0)
		trxId, undoNo, recordType, tableName, columnSets, err := DeserializeUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), trxId)
		assert.Equal(t, uint64(0), undoNo)
		assert.Equal(t, UndoInsert, recordType)
		assert.Equal(t, "test", tableName)
		assert.Equal(t, 1, len(columnSets))
		assert.Equal(t, []byte("a"), columnSets[0][0])
		assert.Equal(t, []byte("John"), columnSets[0][1])
	})
}
