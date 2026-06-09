package access

import (
	"os"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/config"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

func TestNewTrxManager(t *testing.T) {
	t.Run("TrxManager を作成できる", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)

		// WHEN
		redoLog := setupTestRedoLog(t)
		tm := NewTrxManager(env.ct, env.undoLog, redoLog, env.lock, env.bp)

		// THEN
		assert.NotNil(t, tm)
	})
}

func TestTrxManagerBegin(t *testing.T) {
	t.Run("トランザクション ID を払い出す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)

		// WHEN
		trx := tm.Begin()

		// THEN
		assert.Equal(t, lock.TrxId(0), trx.trxId)
	})

	t.Run("連続して呼ぶとインクリメントされた ID を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)

		// WHEN
		trx1 := tm.Begin()
		trx2 := tm.Begin()
		trx3 := tm.Begin()

		// THEN
		assert.Equal(t, lock.TrxId(0), trx1.trxId)
		assert.Equal(t, lock.TrxId(1), trx2.trxId)
		assert.Equal(t, lock.TrxId(2), trx3.trxId)
	})

	t.Run("開始したトランザクションは Active になる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)

		// WHEN
		trx := tm.Begin()

		// THEN
		assert.Equal(t, trxStateActive, tm.transactions[trx.trxId].state)
	})
}

func TestTrxManagerCommit(t *testing.T) {
	t.Run("コミット後にトランザクションが Inactive になる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()

		// WHEN
		err := tm.Commit(trx)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, trxStateInactive, tm.transactions[trx.trxId].state)
	})

	t.Run("コミット後に ReadView がリセットされる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()
		_ = tm.EnsureReadView(trx)

		// WHEN
		_ = tm.Commit(trx)

		// THEN
		assert.Nil(t, trx.readView)
	})
}

func TestTrxManagerRollback(t *testing.T) {
	t.Run("Undo ログがないトランザクションをロールバックできる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()

		// WHEN
		err := tm.Rollback(trx)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, trxStateInactive, tm.transactions[trx.trxId].state)
	})

	t.Run("ロールバック後に ReadView がリセットされる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()
		_ = tm.EnsureReadView(trx)

		// WHEN
		_ = tm.Rollback(trx)

		// THEN
		assert.Nil(t, trx.readView)
	})

	t.Run("Insert のロールバックでレコードが物理削除される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trx)

		// THEN
		assert.NoError(t, err)

		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		iter, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("SoftDelete のロールバックでレコードが復元される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		_ = table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		_ = tm.Commit(trx)

		trx2 := tm.Begin()
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		iter, _ := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		record, _, _ := iter.Next()
		err := table.SoftDelete(trx2, record)
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trx2)

		// THEN
		assert.NoError(t, err)

		iter2, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		restored, ok, err := iter2.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", restored.values[1])
	})

	t.Run("Update のロールバックで旧レコードに復元される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		_ = table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		_ = tm.Commit(trx)

		trx2 := tm.Begin()
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		iter, _ := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		record, _, _ := iter.Next()
		err := table.Update(trx2, record, []string{"name"}, []string{"Bob"})
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trx2)

		// THEN
		assert.NoError(t, err)

		iter2, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		restored, ok, err := iter2.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", restored.values[1])
	})

	t.Run("Insert のロールバックでセカンダリインデックスからも物理削除される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trx)

		// THEN
		assert.NoError(t, err)

		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)

		idxEmail := findSecondaryIndex(t, table, "idx_email")
		emailIter, err := idxEmail.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err = emailIter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("SoftDelete のロールバックでセカンダリインデックスも復元される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		_ = table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		_ = tm.Commit(trx)

		trx2 := tm.Begin()
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		iter, _ := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		record, _, _ := iter.Next()
		err := table.SoftDelete(trx2, record)
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trx2)

		// THEN
		assert.NoError(t, err)
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", nameResult.values[1])
	})

	t.Run("SK が変わる Update のロールバックでセカンダリインデックスが復元される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		_ = table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		_ = tm.Commit(trx)

		trx2 := tm.Begin()
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		iter, _ := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		record, _, _ := iter.Next()
		err := table.Update(trx2, record, []string{"name"}, []string{"Bob"})
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trx2)

		// THEN
		assert.NoError(t, err)
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", nameResult.values[1])
	})

	t.Run("SK が変わらない Update のロールバックではセカンダリインデックスはそのまま", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		_ = table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		_ = tm.Commit(trx)

		trx2 := tm.Begin()
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		iter, _ := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		record, _, _ := iter.Next()
		err := table.Update(trx2, record, []string{"email"}, []string{"new@example.com"})
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trx2)

		// THEN
		assert.NoError(t, err)
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", nameResult.values[1])
	})
}

func TestTrxManagerEnsureReadView(t *testing.T) {
	t.Run("ReadView を作成できる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()

		// WHEN
		rv := tm.EnsureReadView(trx)

		// THEN
		assert.NotNil(t, rv)
		assert.Equal(t, trx.trxId, rv.trxId)
	})

	t.Run("同一トランザクションで 2 回呼ぶとキャッシュされた ReadView を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()

		// WHEN
		rv1 := tm.EnsureReadView(trx)
		rv2 := tm.EnsureReadView(trx)

		// THEN
		assert.Same(t, rv1, rv2)
	})

	t.Run("他のアクティブトランザクションが MIds に含まれる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx1 := tm.Begin()
		trx2 := tm.Begin()

		// WHEN
		rv := tm.EnsureReadView(trx2)

		// THEN
		assert.Contains(t, rv.activeTrxIds, trx1.trxId)
		assert.NotContains(t, rv.activeTrxIds, trx2.trxId)
	})

	t.Run("コミット済みトランザクションは MIds に含まれない", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx1 := tm.Begin()
		_ = tm.Commit(trx1)
		trx2 := tm.Begin()

		// WHEN
		rv := tm.EnsureReadView(trx2)

		// THEN
		assert.NotContains(t, rv.activeTrxIds, trx1.trxId)
	})
}

func TestTrxManagerOldestVisibleTrxId(t *testing.T) {
	t.Run("ReadView がない場合は nextTrxId を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		_ = tm.Begin()

		// WHEN
		oldest := tm.OldestVisibleTrxId()

		// THEN
		assert.Equal(t, lock.TrxId(1), oldest)
	})

	t.Run("ReadView がある場合は MUpLimitId の最小値を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx1 := tm.Begin()
		trx2 := tm.Begin()
		_ = tm.EnsureReadView(trx1)
		_ = tm.EnsureReadView(trx2)

		// WHEN
		oldest := tm.OldestVisibleTrxId()

		// THEN
		assert.Equal(t, lock.TrxId(0), oldest)
	})
}

func TestTrxManagerInactiveTrxIds(t *testing.T) {
	t.Run("コミット済みのトランザクション ID を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx1 := tm.Begin()
		trx2 := tm.Begin()
		_ = tm.Commit(trx1)
		_ = tm.Commit(trx2)

		// WHEN
		ids := tm.InactiveTrxIds()

		// THEN
		assert.Len(t, ids, 2)
		assert.Contains(t, ids, trx1.trxId)
		assert.Contains(t, ids, trx2.trxId)
	})

	t.Run("ロールバック済みのトランザクションも含まれる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx1 := tm.Begin()
		_ = tm.Rollback(trx1)

		// WHEN
		ids := tm.InactiveTrxIds()

		// THEN
		assert.Len(t, ids, 1)
		assert.Contains(t, ids, trx1.trxId)
	})

	t.Run("アクティブなトランザクションは含まれない", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		_ = tm.Begin()

		// WHEN
		ids := tm.InactiveTrxIds()

		// THEN
		assert.Empty(t, ids)
	})

	t.Run("トランザクションがない場合は空を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)

		// WHEN
		ids := tm.InactiveTrxIds()

		// THEN
		assert.Empty(t, ids)
	})
}

func TestTrxManagerActiveTrxIDs(t *testing.T) {
	t.Run("アクティブなトランザクション ID を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx1 := tm.Begin()
		_ = tm.Begin()

		// WHEN
		ids := tm.activeTrxIds()

		// THEN
		assert.Len(t, ids, 2)
		assert.Contains(t, ids, trx1.trxId)
	})

	t.Run("コミット済みトランザクションは含まれない", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx1 := tm.Begin()
		trx2 := tm.Begin()
		_ = tm.Commit(trx1)

		// WHEN
		ids := tm.activeTrxIds()

		// THEN
		assert.Len(t, ids, 1)
		assert.Contains(t, ids, trx2.trxId)
		assert.NotContains(t, ids, trx1.trxId)
	})

	t.Run("アクティブなトランザクションがない場合は空を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)

		// WHEN
		ids := tm.activeTrxIds()

		// THEN
		assert.Empty(t, ids)
	})
}

// setupTrxManager はテスト用の TrxManager を構築する
func setupTrxManager(t *testing.T) *TrxManager {
	t.Helper()
	env := setupTableTestEnv(t)
	redoLog := setupTestRedoLog(t)
	return NewTrxManager(env.ct, env.undoLog, redoLog, env.lock, env.bp)
}

// setupTestRedoLog はテスト用の redo.Buffer を作成する
func setupTestRedoLog(t *testing.T) *redo.Buffer {
	t.Helper()
	_ = os.MkdirAll(config.BaseDir, 0o750)
	t.Cleanup(func() { _ = os.RemoveAll(config.BaseDir) })
	redoLog, err := redo.NewBuffer(config.BaseDir)
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = redoLog.Clear() })
	return redoLog
}

// setupTableForTrxTest は TrxManager のロールバックテスト用に Table を構築する
func setupTableForTrxTest(t *testing.T, tm *TrxManager) *Table {
	t.Helper()
	table, err := NewTable(tm.bufferPool, tm.catalog, tm.undoLog, tm.lock, tm.redoLog, "users")
	if err != nil {
		t.Fatalf("Table の作成に失敗: %v", err)
	}
	return table
}
