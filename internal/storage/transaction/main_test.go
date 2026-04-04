package transaction_test

import (
	"minesql/internal/executor"
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
	"minesql/internal/storage/lock"
	"minesql/internal/storage/transaction"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommit(t *testing.T) {
	t.Run("Commit するとトランザクションが INACTIVE になり Undo ログがクリアされる", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		hdl := handler.Get()
		undoLog := hdl.UndoLog()
		trxMgr := transaction.NewManager(undoLog, lock.NewManager(5000))
		trxId := trxMgr.Begin()
		tbl := setupTestTable(t)

		ins := executor.NewInsert(trxId, tbl, []executor.Record{
			{[]byte("a"), []byte("Alice")},
		})
		_, err := ins.Next()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(undoLog.GetRecords(trxId)))

		// WHEN
		trxMgr.Commit(trxId)

		// THEN
		assert.Equal(t, transaction.StateInactive, trxMgr.Transactions[trxId])
		assert.Equal(t, 0, len(undoLog.GetRecords(trxId)))

		// Commit 後もデータは残っている
		recs := collectAllRecords(t, tbl)
		assert.Equal(t, 1, len(recs))
	})
}

func TestRollback(t *testing.T) {
	t.Run("Insert を Rollback すると行が物理削除される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		hdl := handler.Get()
		undoLog := hdl.UndoLog()
		trxMgr := transaction.NewManager(undoLog, lock.NewManager(5000))
		trxId := trxMgr.Begin()
		tbl := setupTestTable(t)

		ins := executor.NewInsert(trxId, tbl, []executor.Record{
			{[]byte("a"), []byte("Alice")},
			{[]byte("b"), []byte("Bob")},
		})
		_, err := ins.Next()
		assert.NoError(t, err)

		// WHEN
		err = trxMgr.Rollback(hdl.BufferPool, trxId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, transaction.StateInactive, trxMgr.Transactions[trxId])
		recs := collectAllRecords(t, tbl)
		assert.Equal(t, 0, len(recs))
	})

	t.Run("Delete を Rollback すると行が復元される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		tbl := setupTestTable(t)
		hdl := handler.Get()
		undoLog := hdl.UndoLog()
		trxMgr := transaction.NewManager(undoLog, lock.NewManager(5000))

		// 先にデータを挿入して Commit
		insertTrxId := trxMgr.Begin()
		ins := executor.NewInsert(insertTrxId, tbl, []executor.Record{
			{[]byte("a"), []byte("Alice")},
			{[]byte("b"), []byte("Bob")},
		})
		_, err := ins.Next()
		assert.NoError(t, err)
		trxMgr.Commit(insertTrxId)

		// Delete トランザクション
		deleteTrxId := trxMgr.Begin()
		del := executor.NewDelete(deleteTrxId, tbl, executor.NewTableScan(
			0, nil, tbl,
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		))
		_, err = del.Next()
		assert.NoError(t, err)

		// WHEN
		err = trxMgr.Rollback(hdl.BufferPool, deleteTrxId)

		// THEN
		assert.NoError(t, err)
		recs := collectAllRecords(t, tbl)
		assert.Equal(t, 2, len(recs))
	})

	t.Run("Update を Rollback すると行が元の値に戻る", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		tbl := setupTestTable(t)
		hdl := handler.Get()
		undoLog := hdl.UndoLog()
		trxMgr := transaction.NewManager(undoLog, lock.NewManager(5000))

		// 先にデータを挿入して Commit
		insertTrxId := trxMgr.Begin()
		ins := executor.NewInsert(insertTrxId, tbl, []executor.Record{
			{[]byte("a"), []byte("Alice")},
		})
		_, err := ins.Next()
		assert.NoError(t, err)
		trxMgr.Commit(insertTrxId)

		// Update トランザクション
		updateTrxId := trxMgr.Begin()
		upd := executor.NewUpdate(updateTrxId, tbl, []executor.SetColumn{
			{Pos: 1, Value: []byte("Carol")},
		}, executor.NewTableScan(
			0, nil, tbl,
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		))
		_, err = upd.Next()
		assert.NoError(t, err)

		// 更新後の確認
		recs := collectAllRecords(t, tbl)
		assert.Equal(t, "Carol", string(recs[0][1]))

		// WHEN
		err = trxMgr.Rollback(hdl.BufferPool, updateTrxId)

		// THEN
		assert.NoError(t, err)
		recs = collectAllRecords(t, tbl)
		assert.Equal(t, 1, len(recs))
		assert.Equal(t, "Alice", string(recs[0][1]))
	})

	t.Run("複数操作を含むトランザクションの Rollback", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		tbl := setupTestTable(t)
		hdl := handler.Get()
		undoLog := hdl.UndoLog()
		trxMgr := transaction.NewManager(undoLog, lock.NewManager(5000))

		// 先にデータを挿入して Commit
		insertTrxId := trxMgr.Begin()
		ins := executor.NewInsert(insertTrxId, tbl, []executor.Record{
			{[]byte("a"), []byte("Alice")},
			{[]byte("b"), []byte("Bob")},
		})
		_, err := ins.Next()
		assert.NoError(t, err)
		trxMgr.Commit(insertTrxId)

		// 1 つのトランザクション内で Insert + Update + Delete
		trxId := trxMgr.Begin()

		ins2 := executor.NewInsert(trxId, tbl, []executor.Record{
			{[]byte("c"), []byte("Carol")},
		})
		_, err = ins2.Next()
		assert.NoError(t, err)

		upd := executor.NewUpdate(trxId, tbl, []executor.SetColumn{
			{Pos: 1, Value: []byte("Dave")},
		}, executor.NewTableScan(
			0, nil, tbl,
			access.RecordSearchModeKey{Key: [][]byte{[]byte("a")}},
			func(record executor.Record) bool { return string(record[0]) == "a" },
		))
		_, err = upd.Next()
		assert.NoError(t, err)

		del := executor.NewDelete(trxId, tbl, executor.NewFilter(
			executor.NewTableScan(
				0, nil, tbl,
				access.RecordSearchModeStart{},
				func(record executor.Record) bool { return true },
			),
			func(record executor.Record) bool { return string(record[0]) == "b" },
		))
		_, err = del.Next()
		assert.NoError(t, err)

		// WHEN
		err = trxMgr.Rollback(hdl.BufferPool, trxId)

		// THEN: 初期状態に戻る
		assert.NoError(t, err)
		recs := collectAllRecords(t, tbl)
		assert.Equal(t, 2, len(recs))
		assert.Equal(t, "Alice", string(recs[0][1]))
		assert.Equal(t, "Bob", string(recs[1][1]))
	})
}

func initStorageManagerForTest(t *testing.T) {
	t.Helper()
	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "10")
	handler.Reset()
	handler.Init()
}

func setupTestTable(t *testing.T) *access.Table {
	t.Helper()
	createTable := executor.NewCreateTable("test_trx", 1, nil, []handler.CreateColumnParam{
		{Name: "id", Type: handler.ColumnTypeString},
		{Name: "name", Type: handler.ColumnTypeString},
	})
	_, err := createTable.Next()
	assert.NoError(t, err)

	hdl := handler.Get()
	tblMeta, ok := hdl.Catalog.GetTableMetaByName("test_trx")
	assert.True(t, ok)
	tbl, err := tblMeta.GetTable()
	assert.NoError(t, err)
	return tbl
}

func collectAllRecords(t *testing.T, tbl *access.Table) []executor.Record {
	t.Helper()
	scan := executor.NewTableScan(
		0, nil, tbl,
		access.RecordSearchModeStart{},
		func(record executor.Record) bool { return true },
	)
	var recs []executor.Record
	for {
		record, err := scan.Next()
		assert.NoError(t, err)
		if record == nil {
			break
		}
		recs = append(recs, record)
	}
	return recs
}
