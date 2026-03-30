package undo

import (
	"minesql/internal/access"
	"minesql/internal/encode"
	"minesql/internal/engine"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/file"
	"minesql/internal/storage/page"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUndoIntegration(t *testing.T) {
	t.Run("複数操作を逆順に Undo すると元の状態に戻る", func(t *testing.T) {
		// GIVEN: ユニークインデックス付きテーブルにデータを投入
		uniqueIndex := access.NewUniqueIndexAccessMethod("idx_name", "name", page.PageId{}, 1)
		table := setupTestTable(t, []*access.UniqueIndexAccessMethod{uniqueIndex})
		defer engine.Reset()
		bp := engine.Get().BufferPool

		// 初期データ: ("a", "Alice"), ("b", "Bob")
		recordA := [][]byte{[]byte("a"), []byte("Alice")}
		recordB := [][]byte{[]byte("b"), []byte("Bob")}
		err := table.Insert(bp, recordA)
		assert.NoError(t, err)
		err = table.Insert(bp, recordB)
		assert.NoError(t, err)

		// 操作1: ("a", "Alice") を ("a", "Carol") に UpdateInplace
		updatedA := [][]byte{[]byte("a"), []byte("Carol")}
		err = table.UpdateInplace(bp, recordA, updatedA)
		assert.NoError(t, err)
		undo1 := UpdateInplaceLogRecord{table: table, PrevRecord: recordA, NewRecord: updatedA}

		// 操作2: ("b", "Bob") を SoftDelete
		err = table.SoftDelete(bp, recordB)
		assert.NoError(t, err)
		undo2 := DeleteLogRecord{table: table, Record: recordB}

		// 操作3: ("c", "Dave") を Insert
		recordC := [][]byte{[]byte("c"), []byte("Dave")}
		err = table.Insert(bp, recordC)
		assert.NoError(t, err)
		undo3 := InsertLogRecord{table: table, Record: recordC}

		// 操作後の状態: ("a", "Carol"), ("c", "Dave") が active
		records := collectActiveRecords(t, table)
		assert.Equal(t, 2, len(records))
		assert.Equal(t, []string{"a", "Carol"}, records[0])
		assert.Equal(t, []string{"c", "Dave"}, records[1])

		// WHEN: 逆順に Undo
		err = undo3.Undo()
		assert.NoError(t, err)
		err = undo2.Undo()
		assert.NoError(t, err)
		err = undo1.Undo()
		assert.NoError(t, err)

		// THEN: 初期状態に戻っている
		records = collectActiveRecords(t, table)
		assert.Equal(t, 2, len(records))
		assert.Equal(t, []string{"a", "Alice"}, records[0])
		assert.Equal(t, []string{"b", "Bob"}, records[1])

		// THEN: ユニークインデックスも初期状態に戻っている
		keys := collectActiveUniqueIndexKeys(t, table.UniqueIndexes[0])
		assert.Equal(t, []string{"Alice", "Bob"}, keys)
	})

	t.Run("Outofplace 更新を含む複数操作の Undo", func(t *testing.T) {
		// GIVEN
		table := setupTestTable(t, nil)
		defer engine.Reset()
		bp := engine.Get().BufferPool

		// 初期データ: ("a", "Alice")
		recordA := [][]byte{[]byte("a"), []byte("Alice")}
		err := table.Insert(bp, recordA)
		assert.NoError(t, err)

		// 操作1: PK を "a" → "x" に変更 (Outofplace)
		newRecordX := [][]byte{[]byte("x"), []byte("Alice")}
		err = table.SoftDelete(bp, recordA)
		assert.NoError(t, err)
		err = table.Insert(bp, newRecordX)
		assert.NoError(t, err)
		undo1 := UpdateOutofplaceLogRecord{
			InsertLogRecord: InsertLogRecord{table: table, Record: newRecordX},
			DeleteLogRecord: DeleteLogRecord{table: table, Record: recordA},
		}

		// 操作2: ("x", "Alice") を ("x", "Bob") に UpdateInplace
		updatedX := [][]byte{[]byte("x"), []byte("Bob")}
		err = table.UpdateInplace(bp, newRecordX, updatedX)
		assert.NoError(t, err)
		undo2 := UpdateInplaceLogRecord{table: table, PrevRecord: newRecordX, NewRecord: updatedX}

		// 操作後の状態: ("x", "Bob") のみ active
		records := collectActiveRecords(t, table)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, []string{"x", "Bob"}, records[0])

		// WHEN: 逆順に Undo
		err = undo2.Undo()
		assert.NoError(t, err)
		err = undo1.Undo()
		assert.NoError(t, err)

		// THEN: 初期状態に戻っている
		records = collectActiveRecords(t, table)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, []string{"a", "Alice"}, records[0])
	})
}

func setupTestTable(t *testing.T, uniqueIndexes []*access.UniqueIndexAccessMethod) *access.TableAccessMethod {
	t.Helper()
	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "100")
	engine.Reset()
	engine.Init()

	e := engine.Get()
	fileId := page.FileId(1)
	dm, err := file.NewDisk(fileId, filepath.Join(tmpdir, "test.db"))
	assert.NoError(t, err)
	e.BufferPool.RegisterDisk(fileId, dm)

	metaPageId, err := e.BufferPool.AllocatePageId(fileId)
	assert.NoError(t, err)

	for _, ui := range uniqueIndexes {
		indexMetaPageId, err := e.BufferPool.AllocatePageId(fileId)
		assert.NoError(t, err)
		ui.MetaPageId = indexMetaPageId
	}

	table := access.NewTableAccessMethod("test", metaPageId, 1, uniqueIndexes)
	err = table.Create(e.BufferPool)
	assert.NoError(t, err)

	return &table
}

func collectActiveRecords(t *testing.T, table *access.TableAccessMethod) [][]string {
	t.Helper()
	bp := engine.Get().BufferPool
	iter, err := table.Search(bp, access.RecordSearchModeStart{})
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

func collectActiveUniqueIndexKeys(t *testing.T, ui *access.UniqueIndexAccessMethod) []string {
	t.Helper()
	bp := engine.Get().BufferPool
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
