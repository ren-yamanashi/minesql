package undo

import (
	"minesql/internal/access"
	"minesql/internal/engine"
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteLogRecord_Undo(t *testing.T) {
	t.Run("SoftDelete した行が復元される", func(t *testing.T) {
		// GIVEN
		table := setupTestTable(t, nil)
		defer engine.Reset()
		bp := engine.Get().BufferPool

		record := [][]byte{[]byte("a"), []byte("John")}
		err := table.Insert(bp, record)
		assert.NoError(t, err)
		err = table.SoftDelete(bp, record)
		assert.NoError(t, err)

		undoRecord := DeleteLogRecord{table: table, Record: record}

		// WHEN
		err = undoRecord.Undo()

		// THEN: レコードが active に戻っている
		assert.NoError(t, err)
		records := collectActiveRecords(t, table)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, []string{"a", "John"}, records[0])
	})

	t.Run("SoftDelete した行のユニークインデックスも復元される", func(t *testing.T) {
		// GIVEN
		uniqueIndex := access.NewUniqueIndexAccessMethod("idx_name", "name", page.PageId{}, 1)
		table := setupTestTable(t, []*access.UniqueIndexAccessMethod{uniqueIndex})
		defer engine.Reset()
		bp := engine.Get().BufferPool

		record := [][]byte{[]byte("a"), []byte("John")}
		err := table.Insert(bp, record)
		assert.NoError(t, err)
		err = table.SoftDelete(bp, record)
		assert.NoError(t, err)

		undoRecord := DeleteLogRecord{table: table, Record: record}

		// WHEN
		err = undoRecord.Undo()

		// THEN: ユニークインデックスも復元されている
		assert.NoError(t, err)
		keys := collectActiveUniqueIndexKeys(t, table.UniqueIndexes[0])
		assert.Equal(t, []string{"John"}, keys)
	})

	t.Run("物理削除した行が再挿入される", func(t *testing.T) {
		// GIVEN
		table := setupTestTable(t, nil)
		defer engine.Reset()
		bp := engine.Get().BufferPool

		record := [][]byte{[]byte("a"), []byte("John")}
		err := table.Insert(bp, record)
		assert.NoError(t, err)
		err = table.Delete(bp, record)
		assert.NoError(t, err)

		undoRecord := DeleteLogRecord{table: table, Record: record}

		// WHEN
		err = undoRecord.Undo()

		// THEN: レコードが再挿入されている
		assert.NoError(t, err)
		records := collectActiveRecords(t, table)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, []string{"a", "John"}, records[0])
	})
}
