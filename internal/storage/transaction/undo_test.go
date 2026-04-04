package transaction

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
	"minesql/internal/storage/file"
	"minesql/internal/storage/lock"
	"minesql/internal/storage/page"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUndoIntegration(t *testing.T) {
	t.Run("複数操作を逆順に Undo すると元の状態に戻る", func(t *testing.T) {
		// GIVEN: ユニークインデックス付きテーブルにデータを投入
		uniqueIndex := access.NewUniqueIndex("idx_name", "name", page.PageId{}, 1, 1)
		table, bp := setupTestTableForUndo(t, []*access.UniqueIndex{uniqueIndex})

		// 初期データ: ("a", "Alice"), ("b", "Bob")
		recordA := [][]byte{[]byte("a"), []byte("Alice")}
		recordB := [][]byte{[]byte("b"), []byte("Bob")}
		err := table.Insert(bp, recordA)
		assert.NoError(t, err)
		err = table.Insert(bp, recordB)
		assert.NoError(t, err)

		// 操作1: ("a", "Alice") を ("a", "Carol") に UpdateInplace
		updatedA := [][]byte{[]byte("a"), []byte("Carol")}
		err = table.UpdateInplace(bp, 0, lock.NewManager(5000), recordA, updatedA)
		assert.NoError(t, err)
		undo1 := UndoUpdateInplaceRecord{table: table, PrevRecord: recordA, NewRecord: updatedA}

		// 操作2: ("b", "Bob") を SoftDelete
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), recordB)
		assert.NoError(t, err)
		undo2 := UndoDeleteRecord{table: table, Record: recordB}

		// 操作3: ("c", "Dave") を Insert
		recordC := [][]byte{[]byte("c"), []byte("Dave")}
		err = table.Insert(bp, recordC)
		assert.NoError(t, err)
		undo3 := UndoInsertRecord{table: table, Record: recordC}

		// 操作後の状態: ("a", "Carol"), ("c", "Dave") が active
		records := collectActiveRecords(t, table, bp)
		assert.Equal(t, 2, len(records))
		assert.Equal(t, []string{"a", "Carol"}, records[0])
		assert.Equal(t, []string{"c", "Dave"}, records[1])

		// WHEN: 逆順に Undo
		err = undo3.Undo(bp)
		assert.NoError(t, err)
		err = undo2.Undo(bp)
		assert.NoError(t, err)
		err = undo1.Undo(bp)
		assert.NoError(t, err)

		// THEN: 初期状態に戻っている
		records = collectActiveRecords(t, table, bp)
		assert.Equal(t, 2, len(records))
		assert.Equal(t, []string{"a", "Alice"}, records[0])
		assert.Equal(t, []string{"b", "Bob"}, records[1])

		// THEN: ユニークインデックスも初期状態に戻っている
		keys := collectActiveUniqueIndexKeys(t, table.UniqueIndexes[0], bp)
		assert.Equal(t, []string{"Alice", "Bob"}, keys)
	})

	t.Run("Outofplace 更新を含む複数操作の Undo", func(t *testing.T) {
		// GIVEN
		table, bp := setupTestTableForUndo(t, nil)

		// 初期データ: ("a", "Alice")
		recordA := [][]byte{[]byte("a"), []byte("Alice")}
		err := table.Insert(bp, recordA)
		assert.NoError(t, err)

		// 操作1: PK を "a" → "x" に変更 (Outofplace = SoftDelete + Insert)
		newRecordX := [][]byte{[]byte("x"), []byte("Alice")}
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), recordA)
		assert.NoError(t, err)
		err = table.Insert(bp, newRecordX)
		assert.NoError(t, err)
		undo1Delete := UndoDeleteRecord{table: table, Record: recordA}
		undo1Insert := UndoInsertRecord{table: table, Record: newRecordX}

		// 操作2: ("x", "Alice") を ("x", "Bob") に UpdateInplace
		updatedX := [][]byte{[]byte("x"), []byte("Bob")}
		err = table.UpdateInplace(bp, 0, lock.NewManager(5000), newRecordX, updatedX)
		assert.NoError(t, err)
		undo2 := UndoUpdateInplaceRecord{table: table, PrevRecord: newRecordX, NewRecord: updatedX}

		// 操作後の状態: ("x", "Bob") のみ active
		records := collectActiveRecords(t, table, bp)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, []string{"x", "Bob"}, records[0])

		// WHEN: 逆順に Undo (操作2 → 操作1 の Insert → 操作1 の Delete)
		err = undo2.Undo(bp)
		assert.NoError(t, err)
		err = undo1Insert.Undo(bp)
		assert.NoError(t, err)
		err = undo1Delete.Undo(bp)
		assert.NoError(t, err)

		// THEN: 初期状態に戻っている
		records = collectActiveRecords(t, table, bp)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, []string{"a", "Alice"}, records[0])
	})
}

func setupTestTableForUndo(t *testing.T, uniqueIndexes []*access.UniqueIndex) (*access.Table, *buffer.BufferPool) {
	t.Helper()
	tmpdir := t.TempDir()

	bp := buffer.NewBufferPool(100)
	fileId := page.FileId(1)
	dm, err := file.NewDisk(fileId, filepath.Join(tmpdir, "test.db"))
	assert.NoError(t, err)
	bp.RegisterDisk(fileId, dm)

	metaPageId, err := bp.AllocatePageId(fileId)
	assert.NoError(t, err)

	for _, ui := range uniqueIndexes {
		indexMetaPageId, err := bp.AllocatePageId(fileId)
		assert.NoError(t, err)
		ui.MetaPageId = indexMetaPageId
	}

	table := access.NewTable("test", metaPageId, 1, uniqueIndexes)
	err = table.Create(bp)
	assert.NoError(t, err)

	return &table, bp
}

func collectActiveRecords(t *testing.T, table *access.Table, bp *buffer.BufferPool) [][]string {
	t.Helper()
	iter, err := table.Search(bp, 0, lock.NewManager(5000), access.RecordSearchModeStart{})
	assert.NoError(t, err)

	var records [][]string
	for {
		columns, ok, err := iter.Next()
		assert.NoError(t, err)
		if !ok {
			break
		}
		row := make([]string, len(columns))
		for i, col := range columns {
			row[i] = string(col)
		}
		records = append(records, row)
	}
	return records
}

func collectActiveUniqueIndexKeys(t *testing.T, ui *access.UniqueIndex, bp *buffer.BufferPool) []string {
	t.Helper()
	indexTree := btree.NewBTree(ui.MetaPageId)
	indexIter, err := indexTree.Search(bp, btree.SearchModeStart{})
	assert.NoError(t, err)

	var keys []string
	for {
		record, ok := indexIter.Get()
		if !ok {
			break
		}
		if record.HeaderBytes()[0] != 1 {
			var keyColumns [][]byte
			encode.Decode(record.KeyBytes(), &keyColumns)
			keys = append(keys, string(keyColumns[0]))
		}
		_, _, err := indexIter.Next(bp)
		assert.NoError(t, err)
	}
	return keys
}
