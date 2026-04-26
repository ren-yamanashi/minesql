package access

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewTrxManager(t *testing.T) {
	t.Run("空の Manager が生成される", func(t *testing.T) {
		// GIVEN / WHEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)

		// THEN
		assert.NotNil(t, manager)
		assert.Equal(t, 0, len(manager.Transactions))
	})
}

func TestManagerBegin(t *testing.T) {
	t.Run("トランザクションが存在しない場合は TrxId 1 が割り当てられる", func(t *testing.T) {
		// GIVEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)

		// WHEN
		id := manager.Begin()

		// THEN
		assert.Equal(t, lock.TrxId(1), id)
		assert.Equal(t, StateActive, manager.Transactions[id])
	})

	t.Run("連続して Begin すると単調増加する", func(t *testing.T) {
		// GIVEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)

		// WHEN
		id1 := manager.Begin()
		id2 := manager.Begin()
		id3 := manager.Begin()

		// THEN
		assert.Equal(t, lock.TrxId(1), id1)
		assert.Equal(t, lock.TrxId(2), id2)
		assert.Equal(t, lock.TrxId(3), id3)
	})
}

func TestManagerCommit(t *testing.T) {
	t.Run("Commit すると状態が INACTIVE になる", func(t *testing.T) {
		// GIVEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)
		trxId := manager.Begin()

		// WHEN
		err := manager.Commit(trxId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, StateInactive, manager.Transactions[trxId])
	})

	t.Run("Commit すると Undo ログが破棄される", func(t *testing.T) {
		// GIVEN
		_, undoLog, table := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)
		trxId := manager.Begin()
		ptr1, err := undoLog.Append(trxId, UndoInsert, NewUndoInsertRecord(table, [][]byte{[]byte("a"), []byte("Alice")}))
		assert.NoError(t, err)
		ptr2, err := undoLog.Append(trxId, UndoInsert, NewUndoInsertRecord(table, [][]byte{[]byte("b"), []byte("Bob")}))
		assert.NoError(t, err)
		assert.Equal(t, 2, len(undoLog.GetRecords(trxId)))
		// Append が有効な UndoPtr を返すことを確認
		assert.False(t, ptr1.IsNull())
		assert.False(t, ptr2.IsNull())
		assert.Greater(t, ptr2.Offset, ptr1.Offset)

		// WHEN
		err = manager.Commit(trxId)

		// THEN
		assert.NoError(t, err)
		assert.Nil(t, undoLog.GetRecords(trxId))
	})
	t.Run("Commit すると INSERT の undo ログは破棄されるが UPDATE/DELETE の undo ログは残る", func(t *testing.T) {
		// GIVEN
		bp, undoLog, table := initManagerTest(t)
		lockMgr := lock.NewManager(5000)
		table.undoLog = undoLog
		manager := NewTrxManager(undoLog, lockMgr, nil)
		trxId := manager.Begin()

		// INSERT → UPDATE → DELETE の undo レコードを記録
		err := table.Insert(bp, trxId, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.UpdateInplace(bp, trxId, lockMgr,
			[][]byte{[]byte("a"), []byte("Alice")},
			[][]byte{[]byte("a"), []byte("Bob")},
		)
		assert.NoError(t, err)
		err = table.Insert(bp, trxId, lockMgr, [][]byte{[]byte("b"), []byte("Carol")})
		assert.NoError(t, err)
		err = table.SoftDelete(bp, trxId, lockMgr, [][]byte{[]byte("b"), []byte("Carol")})
		assert.NoError(t, err)
		assert.Equal(t, 4, len(undoLog.GetRecords(trxId)))

		// WHEN
		err = manager.Commit(trxId)

		// THEN: INSERT の undo レコード (2 件) は破棄され、UPDATE + DELETE の undo レコード (2 件) が残る
		assert.NoError(t, err)
		records := undoLog.GetRecords(trxId)
		assert.Equal(t, 2, len(records))
		_, isUpdate := records[0].(UndoUpdateInplaceRecord)
		assert.True(t, isUpdate)
		_, isDelete := records[1].(UndoDeleteRecord)
		assert.True(t, isDelete)
	})
}

func TestManagerRollback(t *testing.T) {
	t.Run("Rollback すると Undo ログが逆順に適用される", func(t *testing.T) {
		// GIVEN
		bp, undoLog, table := initManagerTest(t)
		lockMgr := lock.NewManager(5000)
		table.undoLog = undoLog
		manager := NewTrxManager(undoLog, lockMgr, nil)
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
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)
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
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)
		trxId := manager.Begin()

		// テーブルに存在しない行の削除 Undo (= 存在しない行を insertRaw しようとする)
		_, err := undoLog.Append(trxId, UndoDelete, NewUndoDeleteRecord(table, [][]byte{[]byte("nonexistent"), []byte("data")}, 0, NullUndoPtr))
		assert.NoError(t, err)

		// さらに Insert の Undo (= 存在しない行を deleteRaw しようとする) を追加
		_, err = undoLog.Append(trxId, UndoInsert, NewUndoInsertRecord(table, [][]byte{[]byte("nonexistent"), []byte("data")}))
		assert.NoError(t, err)

		// WHEN
		err = manager.Rollback(bp, trxId)

		// THEN
		assert.Error(t, err)
	})

	t.Run("他のトランザクションの Undo ログには影響しない", func(t *testing.T) {
		// GIVEN
		bp, undoLog, table := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)
		trx1 := manager.Begin()
		trx2 := manager.Begin()

		err := table.Insert(bp, trx1, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		_, err = undoLog.Append(trx2, UndoInsert, NewUndoInsertRecord(table, [][]byte{[]byte("b"), []byte("Bob")}))
		assert.NoError(t, err)

		// WHEN
		err = manager.Rollback(bp, trx1)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(undoLog.GetRecords(trx2)))
	})
}

func TestManagerCreateReadView(t *testing.T) {
	t.Run("自分以外のアクティブトランザクションが MIds に含まれる", func(t *testing.T) {
		// GIVEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)
		trx1 := manager.Begin() // TrxId=1
		trx2 := manager.Begin() // TrxId=2
		trx3 := manager.Begin() // TrxId=3

		// WHEN
		rv := manager.CreateReadView(trx2)

		// THEN
		assert.Equal(t, trx2, rv.TrxId)
		assert.Equal(t, lock.TrxId(4), rv.MLowLimitId) // nextTrxId
		assert.Contains(t, rv.MIds, trx1)
		assert.Contains(t, rv.MIds, trx3)
		assert.NotContains(t, rv.MIds, trx2) // 自分は含まれない
	})

	t.Run("コミット済みトランザクションは MIds に含まれない", func(t *testing.T) {
		// GIVEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)
		trx1 := manager.Begin() // TrxId=1
		trx2 := manager.Begin() // TrxId=2
		_ = manager.Commit(trx1)
		trx3 := manager.Begin() // TrxId=3

		// WHEN
		rv := manager.CreateReadView(trx3)

		// THEN
		assert.Equal(t, trx3, rv.TrxId)
		assert.NotContains(t, rv.MIds, trx1) // コミット済み
		assert.Contains(t, rv.MIds, trx2)    // アクティブ
		// MUpLimitId は trx2 (アクティブの最小)
		assert.Equal(t, trx2, rv.MUpLimitId)
	})

	t.Run("他にアクティブトランザクションがない場合 MUpLimitId は nextTrxId", func(t *testing.T) {
		// GIVEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)
		trx1 := manager.Begin()
		_ = trx1

		// WHEN
		rv := manager.CreateReadView(trx1)

		// THEN
		assert.Equal(t, 0, len(rv.MIds))
		assert.Equal(t, rv.MLowLimitId, rv.MUpLimitId)
	})
}

func TestPurgeLimit(t *testing.T) {
	t.Run("アクティブな ReadView がない場合は nextTrxId を返す", func(t *testing.T) {
		// GIVEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)
		trx1 := manager.Begin()
		_ = manager.Commit(trx1)

		// WHEN
		limit := manager.PurgeLimit()

		// THEN: nextTrxId=2 (全コミット済みトランザクションがパージ可能)
		assert.Equal(t, lock.TrxId(2), limit)
	})

	t.Run("アクティブな ReadView の MUpLimitId の最小値を返す", func(t *testing.T) {
		// GIVEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)
		trx1 := manager.Begin() // TrxId=1
		_ = manager.Commit(trx1)
		trx2 := manager.Begin() // TrxId=2
		trx3 := manager.Begin() // TrxId=3

		// T2 と T3 が ReadView を作成
		manager.CreateReadView(trx2) // MUpLimitId=3 (T3 がアクティブ)
		manager.CreateReadView(trx3) // MUpLimitId=2 (T2 がアクティブ)

		// WHEN
		limit := manager.PurgeLimit()

		// THEN: min(3, 2) = 2
		assert.Equal(t, lock.TrxId(2), limit)
	})

	t.Run("ReadView が 1 つの場合はその MUpLimitId を返す", func(t *testing.T) {
		// GIVEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)
		trx1 := manager.Begin() // TrxId=1
		_ = manager.Commit(trx1)
		trx2 := manager.Begin() // TrxId=2
		manager.CreateReadView(trx2)

		// WHEN
		limit := manager.PurgeLimit()

		// THEN: T1 はコミット済みなので MUpLimitId=nextTrxId=3
		assert.Equal(t, lock.TrxId(3), limit)
	})
}

func TestCommittedTrxIds(t *testing.T) {
	t.Run("コミット済みトランザクションの ID を返す", func(t *testing.T) {
		// GIVEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)
		trx1 := manager.Begin()
		trx2 := manager.Begin()
		_ = manager.Begin() // trx3 (アクティブ)
		_ = manager.Commit(trx1)
		_ = manager.Commit(trx2)

		// WHEN
		ids := manager.CommittedTrxIds()

		// THEN
		assert.Equal(t, 2, len(ids))
		assert.Contains(t, ids, trx1)
		assert.Contains(t, ids, trx2)
	})
}

func TestSetNextTrxId(t *testing.T) {
	t.Run("指定値が現在の nextTrxId より大きい場合に更新される", func(t *testing.T) {
		// GIVEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)
		// nextTrxId は初期値 1

		// WHEN
		manager.SetNextTrxId(10)

		// THEN: 次の Begin で trxId=10 が払い出される
		trxId := manager.Begin()
		assert.Equal(t, lock.TrxId(10), trxId)
	})

	t.Run("指定値が現在の nextTrxId 以下の場合は更新されない", func(t *testing.T) {
		// GIVEN
		_, undoLog, _ := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)
		manager.Begin() // trxId=1 を消費 → nextTrxId=2
		manager.Begin() // trxId=2 を消費 → nextTrxId=3

		// WHEN: nextTrxId(3) より小さい値を指定
		manager.SetNextTrxId(1)

		// THEN: nextTrxId は変わらず、次の Begin で trxId=3 が払い出される
		trxId := manager.Begin()
		assert.Equal(t, lock.TrxId(3), trxId)
	})

	t.Run("SetNextTrxId 後の ReadView が過去の trxId を可視と判定する", func(t *testing.T) {
		// GIVEN: サーバー再起動を模擬 — 過去に trxId=5 でコミットされたレコードがある想定
		_, undoLog, _ := initManagerTest(t)
		manager := NewTrxManager(undoLog, lock.NewManager(5000), nil)
		// nextTrxId を 6 に設定 (過去の最大 trxId=5 + 1)
		manager.SetNextTrxId(6)

		// WHEN
		trxId := manager.Begin() // trxId=6
		rv := manager.CreateReadView(trxId)

		// THEN: 過去の trxId=5 は MUpLimitId(6) 未満なので可視
		assert.True(t, rv.IsVisible(5))
		// trxId=6 は自分自身なので可視
		assert.True(t, rv.IsVisible(6))
		// trxId=7 は MLowLimitId(7) 以上なので不可視
		assert.False(t, rv.IsVisible(7))
	})
}

// initManagerTest はテスト用にバッファプール・UndoManager・テーブルを初期化する
func initManagerTest(t *testing.T) (*buffer.BufferPool, *UndoManager, *Table) {
	t.Helper()
	tmpdir := t.TempDir()
	bp := buffer.NewBufferPool(100, nil)

	// UNDO 用 Disk
	undoDm, err := file.NewDisk(undoTestFileId, filepath.Join(tmpdir, "undo.db"))
	assert.NoError(t, err)
	bp.RegisterDisk(undoTestFileId, undoDm)

	// テーブル用 Disk
	tableDm, err := file.NewDisk(page.FileId(1), filepath.Join(tmpdir, "test.db"))
	assert.NoError(t, err)
	bp.RegisterDisk(page.FileId(1), tableDm)

	undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
	assert.NoError(t, err)

	metaPageId, err := bp.AllocatePageId(page.FileId(1))
	assert.NoError(t, err)
	table := NewTable("test_mgr", metaPageId, 1, nil, nil, nil)
	err = table.Create(bp)
	assert.NoError(t, err)

	return bp, undoLog, &table
}
