package access

import (
	"minesql/internal/storage/lock"
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUndoDeleteRecord_Undo(t *testing.T) {
	t.Run("SoftDelete した行が復元される", func(t *testing.T) {
		// GIVEN
		table, bp := setupTestTableForUndo(t, nil)

		record := [][]byte{[]byte("a"), []byte("John")}
		err := table.Insert(bp, 0, lock.NewManager(5000), record)
		assert.NoError(t, err)
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), record)
		assert.NoError(t, err)

		undoRecord := NewUndoDeleteRecord(table, record)

		// WHEN
		err = undoRecord.Undo(bp, 0, lock.NewManager(5000))

		// THEN: レコードが active に戻っている
		assert.NoError(t, err)
		records := collectUndoActiveRecords(t, table, bp)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, []string{"a", "John"}, records[0])
	})

	t.Run("SoftDelete した行のユニークインデックスも復元される", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewUniqueIndex("idx_name", "name", page.PageId{}, 1, 1)
		table, bp := setupTestTableForUndo(t, []*UniqueIndex{uniqueIndex})

		record := [][]byte{[]byte("a"), []byte("John")}
		err := table.Insert(bp, 0, lock.NewManager(5000), record)
		assert.NoError(t, err)
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), record)
		assert.NoError(t, err)

		undoRecord := NewUndoDeleteRecord(table, record)

		// WHEN
		err = undoRecord.Undo(bp, 0, lock.NewManager(5000))

		// THEN: ユニークインデックスも復元されている
		assert.NoError(t, err)
		keys := collectUndoActiveUniqueIndexKeys(t, table.UniqueIndexes[0], bp)
		assert.Equal(t, []string{"John"}, keys)
	})

	t.Run("物理削除した行が再挿入される", func(t *testing.T) {
		// GIVEN
		table, bp := setupTestTableForUndo(t, nil)

		record := [][]byte{[]byte("a"), []byte("John")}
		err := table.Insert(bp, 0, lock.NewManager(5000), record)
		assert.NoError(t, err)
		err = table.delete(bp, 0, lock.NewManager(5000), record)
		assert.NoError(t, err)

		undoRecord := NewUndoDeleteRecord(table, record)

		// WHEN
		err = undoRecord.Undo(bp, 0, lock.NewManager(5000))

		// THEN: レコードが再挿入されている
		assert.NoError(t, err)
		records := collectUndoActiveRecords(t, table, bp)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, []string{"a", "John"}, records[0])
	})
}

func TestUndoDeleteRecord_Serialize(t *testing.T) {
	t.Run("シリアライズしてデシリアライズすると元のデータが復元される", func(t *testing.T) {
		// GIVEN
		table, _ := setupTestTableForUndo(t, nil)
		record := NewUndoDeleteRecord(table, [][]byte{[]byte("a"), []byte("John")})

		// WHEN
		buf := record.Serialize(2, 1)
		trxId, undoNo, recordType, tableName, columnSets, err := DeserializeUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), trxId)
		assert.Equal(t, uint64(1), undoNo)
		assert.Equal(t, UndoDelete, recordType)
		assert.Equal(t, "test", tableName)
		assert.Equal(t, 1, len(columnSets))
		assert.Equal(t, []byte("a"), columnSets[0][0])
		assert.Equal(t, []byte("John"), columnSets[0][1])
	})
}
