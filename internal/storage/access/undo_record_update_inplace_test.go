package access

import (
	"minesql/internal/storage/lock"
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUndoUpdateInplaceRecord_Undo(t *testing.T) {
	t.Run("UpdateInplace した行が元の値に戻る", func(t *testing.T) {
		// GIVEN
		table, bp := setupTestTableForUndo(t, nil)

		prevRecord := [][]byte{[]byte("a"), []byte("John")}
		newRecord := [][]byte{[]byte("a"), []byte("Jane")}
		err := table.Insert(bp, 0, lock.NewManager(5000), prevRecord)
		assert.NoError(t, err)
		err = table.UpdateInplace(bp, 0, lock.NewManager(5000), prevRecord, newRecord)
		assert.NoError(t, err)

		undoRecord := NewUndoUpdateInplaceRecord(table, prevRecord, newRecord)

		// WHEN
		err = undoRecord.Undo(bp, 0, lock.NewManager(5000))

		// THEN: 元の値に戻っている
		assert.NoError(t, err)
		records := collectUndoActiveRecords(t, table, bp)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, []string{"a", "John"}, records[0])
	})

	t.Run("ユニークインデックスも元の値に戻る", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewUniqueIndex("idx_name", "name", page.PageId{}, 1, 1)
		table, bp := setupTestTableForUndo(t, []*UniqueIndex{uniqueIndex})

		prevRecord := [][]byte{[]byte("a"), []byte("John")}
		newRecord := [][]byte{[]byte("a"), []byte("Jane")}
		err := table.Insert(bp, 0, lock.NewManager(5000), prevRecord)
		assert.NoError(t, err)
		err = table.UpdateInplace(bp, 0, lock.NewManager(5000), prevRecord, newRecord)
		assert.NoError(t, err)

		undoRecord := NewUndoUpdateInplaceRecord(table, prevRecord, newRecord)

		// WHEN
		err = undoRecord.Undo(bp, 0, lock.NewManager(5000))

		// THEN: ユニークインデックスも元の値に戻っている
		assert.NoError(t, err)
		keys := collectUndoActiveUniqueIndexKeys(t, table.UniqueIndexes[0], bp)
		assert.Equal(t, []string{"John"}, keys)
	})
}

func TestUndoUpdateInplaceRecord_Serialize(t *testing.T) {
	t.Run("シリアライズしてデシリアライズすると元のデータが復元される", func(t *testing.T) {
		// GIVEN
		table, _ := setupTestTableForUndo(t, nil)
		prevCols := [][]byte{[]byte("a"), []byte("John")}
		newCols := [][]byte{[]byte("a"), []byte("Jane")}
		record := NewUndoUpdateInplaceRecord(table, prevCols, newCols)

		// WHEN
		buf := record.Serialize(3, 2)
		trxId, undoNo, recordType, tableName, columnSets, err := DeserializeUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), trxId)
		assert.Equal(t, uint64(2), undoNo)
		assert.Equal(t, UndoUpdateInplace, recordType)
		assert.Equal(t, "test", tableName)
		assert.Equal(t, 2, len(columnSets))
		assert.Equal(t, []byte("John"), columnSets[0][1])
		assert.Equal(t, []byte("Jane"), columnSets[1][1])
	})
}
