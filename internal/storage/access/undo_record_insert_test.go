package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
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
		_, ok := iter.Get()
		assert.False(t, ok)
	})

	t.Run("Insert した行のユニークインデックスも物理削除される", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewSecondaryIndex("idx_name", "name", page.PageId{}, 1, 1, true)
		table, bp := setupTestTableForUndo(t, []*SecondaryIndex{uniqueIndex})

		record := [][]byte{[]byte("a"), []byte("John")}
		err := table.Insert(bp, 0, lock.NewManager(5000), record)
		assert.NoError(t, err)

		undoRecord := NewUndoInsertRecord(table, record)

		// WHEN
		err = undoRecord.Undo(bp, 0, lock.NewManager(5000))

		// THEN: ユニークインデックスからも物理削除されている
		assert.NoError(t, err)
		keys := collectUndoActiveSecondaryIndexKeys(t, table.SecondaryIndexes[0], bp)
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
		f, err := DeserializeUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), f.TrxId)
		assert.Equal(t, uint64(0), f.UndoNo)
		assert.Equal(t, UndoInsert, f.RecordType)
		assert.Equal(t, "test", f.TableName)
		assert.Equal(t, 1, len(f.ColumnSets))
		assert.Equal(t, []byte("a"), f.ColumnSets[0][0])
		assert.Equal(t, []byte("John"), f.ColumnSets[0][1])
	})
}
