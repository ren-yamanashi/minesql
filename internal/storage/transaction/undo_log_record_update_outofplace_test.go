package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateOutofplaceLogRecord_Undo(t *testing.T) {
	t.Run("Outofplace 更新した行が元の状態に戻る", func(t *testing.T) {
		// GIVEN: PK を "a" → "b" に変更 (SoftDelete + Insert)
		table, bp := setupTestTableForUndo(t, nil)

		oldRecord := [][]byte{[]byte("a"), []byte("John")}
		newRecord := [][]byte{[]byte("b"), []byte("Jane")}
		err := table.Insert(bp, oldRecord)
		assert.NoError(t, err)
		err = table.SoftDelete(bp, oldRecord)
		assert.NoError(t, err)
		err = table.Insert(bp, newRecord)
		assert.NoError(t, err)

		undoRecord := UpdateOutofplaceLogRecord{
			InsertLogRecord: InsertLogRecord{table: table, Record: newRecord},
			DeleteLogRecord: DeleteLogRecord{table: table, Record: oldRecord},
		}

		// WHEN
		err = undoRecord.Undo(bp)

		// THEN: "b" が物理削除され、"a" が active に戻っている
		assert.NoError(t, err)
		records := collectActiveRecords(t, table, bp)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, []string{"a", "John"}, records[0])
	})
}
