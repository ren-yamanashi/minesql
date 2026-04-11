package access

import (
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/file"
	"minesql/internal/storage/lock"
	"minesql/internal/storage/page"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewManager(t *testing.T) {
	t.Run("空の Manager が生成される", func(t *testing.T) {
		// GIVEN / WHEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewManager(undoLog, lock.NewManager(5000))

		// THEN
		assert.NotNil(t, manager)
		assert.Equal(t, 0, len(manager.Transactions))
	})
}

func TestManagerBegin(t *testing.T) {
	t.Run("トランザクションが存在しない場合は TrxId 1 が割り当てられる", func(t *testing.T) {
		// GIVEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewManager(undoLog, lock.NewManager(5000))

		// WHEN
		id := manager.Begin()

		// THEN
		assert.Equal(t, TrxId(1), id)
		assert.Equal(t, StateActive, manager.Transactions[id])
	})

	t.Run("連続して Begin すると単調増加する", func(t *testing.T) {
		// GIVEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewManager(undoLog, lock.NewManager(5000))

		// WHEN
		id1 := manager.Begin()
		id2 := manager.Begin()
		id3 := manager.Begin()

		// THEN
		assert.Equal(t, TrxId(1), id1)
		assert.Equal(t, TrxId(2), id2)
		assert.Equal(t, TrxId(3), id3)
	})
}

func TestManagerCommit(t *testing.T) {
	t.Run("Commit すると状態が INACTIVE になる", func(t *testing.T) {
		// GIVEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewManager(undoLog, lock.NewManager(5000))
		trxId := manager.Begin()

		// WHEN
		manager.Commit(trxId)

		// THEN
		assert.Equal(t, StateInactive, manager.Transactions[trxId])
	})

	t.Run("Commit すると Undo ログが破棄される", func(t *testing.T) {
		// GIVEN
		_, undoLog, table := initManagerTest(t)
		manager := NewManager(undoLog, lock.NewManager(5000))
		trxId := manager.Begin()
		err := undoLog.Append(trxId, NewUndoInsertRecord(table, [][]byte{[]byte("a"), []byte("Alice")}))
		assert.NoError(t, err)
		err = undoLog.Append(trxId, NewUndoInsertRecord(table, [][]byte{[]byte("b"), []byte("Bob")}))
		assert.NoError(t, err)
		assert.Equal(t, 2, len(undoLog.GetRecords(trxId)))

		// WHEN
		manager.Commit(trxId)

		// THEN
		assert.Nil(t, undoLog.GetRecords(trxId))
	})
}

func TestManagerRollback(t *testing.T) {
	t.Run("Rollback すると Undo ログが逆順に適用される", func(t *testing.T) {
		// GIVEN
		bp, undoLog, table := initManagerTest(t)
		lockMgr := lock.NewManager(5000)
		table.undoLog = undoLog
		manager := NewManager(undoLog, lockMgr)
		trxId := manager.Begin()

		err := table.Insert(bp, trxId, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.Insert(bp, trxId, lockMgr, [][]byte{[]byte("b"), []byte("Bob")})
		assert.NoError(t, err)
		err = table.Insert(bp, trxId, lockMgr, [][]byte{[]byte("c"), []byte("Carol")})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(undoLog.GetRecords(trxId)))

		// WHEN
		err = manager.Rollback(bp, trxId)

		// THEN
		assert.NoError(t, err)
		records := collectUndoActiveRecords(t, table, bp)
		assert.Equal(t, 0, len(records))
	})

	t.Run("Rollback すると状態が INACTIVE になり Undo ログが破棄される", func(t *testing.T) {
		// GIVEN
		bp, undoLog, table := initManagerTest(t)
		manager := NewManager(undoLog, lock.NewManager(5000))
		trxId := manager.Begin()

		// テーブルにデータを挿入してから Undo レコードを記録
		err := table.Insert(bp, trxId, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		// WHEN
		err = manager.Rollback(bp, trxId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, StateInactive, manager.Transactions[trxId])
		assert.Nil(t, undoLog.GetRecords(trxId))
	})

	t.Run("Undo がエラーを返した場合、Rollback もエラーを返す", func(t *testing.T) {
		// GIVEN
		bp, undoLog, table := initManagerTest(t)
		manager := NewManager(undoLog, lock.NewManager(5000))
		trxId := manager.Begin()

		// テーブルに存在しない行の削除 Undo (= 存在しない行を insertRaw しようとする)
		err := undoLog.Append(trxId, NewUndoDeleteRecord(table, [][]byte{[]byte("nonexistent"), []byte("data")}))
		assert.NoError(t, err)

		// さらに Insert の Undo (= 存在しない行を deleteRaw しようとする) を追加
		err = undoLog.Append(trxId, NewUndoInsertRecord(table, [][]byte{[]byte("nonexistent"), []byte("data")}))
		assert.NoError(t, err)

		// WHEN
		err = manager.Rollback(bp, trxId)

		// THEN
		assert.Error(t, err)
	})

	t.Run("他のトランザクションの Undo ログには影響しない", func(t *testing.T) {
		// GIVEN
		bp, undoLog, table := initManagerTest(t)
		manager := NewManager(undoLog, lock.NewManager(5000))
		trx1 := manager.Begin()
		trx2 := manager.Begin()

		err := table.Insert(bp, trx1, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		err = undoLog.Append(trx2, NewUndoInsertRecord(table, [][]byte{[]byte("b"), []byte("Bob")}))
		assert.NoError(t, err)

		// WHEN
		err = manager.Rollback(bp, trx1)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(undoLog.GetRecords(trx2)))
	})
}

// initManagerTest はテスト用にバッファプール・UndoLog・テーブルを初期化する
func initManagerTest(t *testing.T) (*buffer.BufferPool, *UndoLog, *Table) {
	t.Helper()
	tmpdir := t.TempDir()
	bp := buffer.NewBufferPool(100)

	// UNDO 用 Disk
	undoDm, err := file.NewDisk(undoTestFileId, filepath.Join(tmpdir, "undo.db"))
	assert.NoError(t, err)
	bp.RegisterDisk(undoTestFileId, undoDm)

	// テーブル用 Disk
	tableDm, err := file.NewDisk(page.FileId(1), filepath.Join(tmpdir, "test.db"))
	assert.NoError(t, err)
	bp.RegisterDisk(page.FileId(1), tableDm)

	undoLog, err := NewUndoLog(bp, nil, undoTestFileId)
	assert.NoError(t, err)

	metaPageId, err := bp.AllocatePageId(page.FileId(1))
	assert.NoError(t, err)
	table := NewTable("test_mgr", metaPageId, 1, nil, nil)
	err = table.Create(bp)
	assert.NoError(t, err)

	return bp, undoLog, &table
}
