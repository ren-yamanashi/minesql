package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/stretchr/testify/assert"
)

func TestNewTrxManager(t *testing.T) {
	t.Run("TrxManager を作成できる", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)

		// WHEN
		tm := NewTrxManager(env.ct, env.undoLog, env.lock, env.bp)

		// THEN
		assert.NotNil(t, tm)
	})
}

func TestTrxManagerBegin(t *testing.T) {
	t.Run("トランザクション ID を払い出す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)

		// WHEN
		trxId := tm.Begin()

		// THEN
		assert.Equal(t, lock.TrxId(0), trxId)
	})

	t.Run("連続して呼ぶとインクリメントされた ID を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)

		// WHEN
		id1 := tm.Begin()
		id2 := tm.Begin()
		id3 := tm.Begin()

		// THEN
		assert.Equal(t, lock.TrxId(0), id1)
		assert.Equal(t, lock.TrxId(1), id2)
		assert.Equal(t, lock.TrxId(2), id3)
	})

	t.Run("開始したトランザクションは Active になる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)

		// WHEN
		trxId := tm.Begin()

		// THEN
		assert.Equal(t, TrxStateActive, tm.transactions[trxId])
	})
}

func TestTrxManagerCommit(t *testing.T) {
	t.Run("コミット後にトランザクションが Inactive になる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trxId := tm.Begin()

		// WHEN
		err := tm.Commit(trxId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, TrxStateInactive, tm.transactions[trxId])
	})

	t.Run("コミット後に ReadView が削除される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trxId := tm.Begin()
		_ = tm.CreateReadView(trxId)

		// WHEN
		_ = tm.Commit(trxId)

		// THEN
		_, ok := tm.readViews[trxId]
		assert.False(t, ok)
	})
}

func TestTrxManagerRollback(t *testing.T) {
	t.Run("Undo ログがないトランザクションをロールバックできる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trxId := tm.Begin()

		// WHEN
		err := tm.Rollback(trxId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, TrxStateInactive, tm.transactions[trxId])
	})

	t.Run("ロールバック後に ReadView が削除される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trxId := tm.Begin()
		_ = tm.CreateReadView(trxId)

		// WHEN
		_ = tm.Rollback(trxId)

		// THEN
		_, ok := tm.readViews[trxId]
		assert.False(t, ok)
	})

	t.Run("Insert のロールバックでレコードが物理削除される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trxId := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId,
		)
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trxId)

		// THEN
		assert.NoError(t, err)

		// レコードが存在しないことを確認
		iter, err := table.primaryIndex.Search(SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("SoftDelete のロールバックでレコードが復元される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trxId := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		_ = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId,
		)
		_ = tm.Commit(trxId)

		trxId2 := tm.Begin()
		iter, _ := table.primaryIndex.Search(SearchModeStart{})
		record, _, _ := iter.Next()
		err := table.SoftDelete(record, trxId2)
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trxId2)

		// THEN
		assert.NoError(t, err)

		// レコードが復元されていることを確認
		iter2, err := table.primaryIndex.Search(SearchModeStart{})
		assert.NoError(t, err)
		restored, ok, err := iter2.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", restored.Values[1])
	})

	t.Run("Update のロールバックで旧レコードに復元される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trxId := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		_ = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId,
		)
		_ = tm.Commit(trxId)

		trxId2 := tm.Begin()
		iter, _ := table.primaryIndex.Search(SearchModeStart{})
		record, _, _ := iter.Next()
		err := table.Update(record, []string{"name"}, []string{"Bob"}, trxId2)
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trxId2)

		// THEN
		assert.NoError(t, err)

		// 旧レコードに復元されていることを確認
		iter2, err := table.primaryIndex.Search(SearchModeStart{})
		assert.NoError(t, err)
		restored, ok, err := iter2.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", restored.Values[1])
	})
}

func TestTrxManagerCreateReadView(t *testing.T) {
	t.Run("ReadView を作成できる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trxId := tm.Begin()

		// WHEN
		rv := tm.CreateReadView(trxId)

		// THEN
		assert.NotNil(t, rv)
		assert.Equal(t, trxId, rv.TrxId)
	})

	t.Run("同一トランザクションで 2 回呼ぶとキャッシュされた ReadView を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trxId := tm.Begin()

		// WHEN
		rv1 := tm.CreateReadView(trxId)
		rv2 := tm.CreateReadView(trxId)

		// THEN
		assert.Same(t, rv1, rv2)
	})

	t.Run("他のアクティブトランザクションが MIds に含まれる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		id1 := tm.Begin()
		id2 := tm.Begin()

		// WHEN
		rv := tm.CreateReadView(id2)

		// THEN
		assert.Contains(t, rv.MIds, id1)
		assert.NotContains(t, rv.MIds, id2)
	})

	t.Run("コミット済みトランザクションは MIds に含まれない", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		id1 := tm.Begin()
		_ = tm.Commit(id1)
		id2 := tm.Begin()

		// WHEN
		rv := tm.CreateReadView(id2)

		// THEN
		assert.NotContains(t, rv.MIds, id1)
	})
}

func TestTrxManagerPurgeLimit(t *testing.T) {
	t.Run("ReadView がない場合は nextTrxId を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		_ = tm.Begin() // nextTrxId = 1

		// WHEN
		limit := tm.PurgeLimit()

		// THEN
		assert.Equal(t, lock.TrxId(1), limit)
	})

	t.Run("ReadView がある場合は MUpLimitId の最小値を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		id1 := tm.Begin() // trxId=0
		id2 := tm.Begin() // trxId=1
		_ = tm.CreateReadView(id1)
		_ = tm.CreateReadView(id2)

		// WHEN
		limit := tm.PurgeLimit()

		// THEN
		// id1 の ReadView: MUpLimitId = min(activeTrxIds except id1) = id2 = 1
		// id2 の ReadView: MUpLimitId = min(activeTrxIds except id2) = id1 = 0
		assert.Equal(t, lock.TrxId(0), limit)
	})
}

func TestTrxManagerActiveTrxIds(t *testing.T) {
	t.Run("アクティブなトランザクション ID を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		id1 := tm.Begin()
		_ = tm.Begin()

		// WHEN
		ids := tm.ActiveTrxIds()

		// THEN
		assert.Len(t, ids, 2)
		assert.Contains(t, ids, id1)
	})

	t.Run("コミット済みトランザクションは含まれない", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		id1 := tm.Begin()
		id2 := tm.Begin()
		_ = tm.Commit(id1)

		// WHEN
		ids := tm.ActiveTrxIds()

		// THEN
		assert.Len(t, ids, 1)
		assert.Contains(t, ids, id2)
		assert.NotContains(t, ids, id1)
	})

	t.Run("アクティブなトランザクションがない場合は空を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)

		// WHEN
		ids := tm.ActiveTrxIds()

		// THEN
		assert.Empty(t, ids)
	})
}

// setupTrxManager はテスト用の TrxManager を構築する
func setupTrxManager(t *testing.T) *TrxManager {
	t.Helper()
	env := setupTableTestEnv(t)
	return NewTrxManager(env.ct, env.undoLog, env.lock, env.bp)
}

// setupTableForTrxTest は TrxManager のロールバックテスト用に Table を構築する
func setupTableForTrxTest(t *testing.T, tm *TrxManager) *Table {
	t.Helper()
	table, err := NewTable(tm.bufferPool, tm.catalog, tm.undoLog, tm.lock, "users")
	if err != nil {
		t.Fatalf("Table の作成に失敗: %v", err)
	}
	return table
}
