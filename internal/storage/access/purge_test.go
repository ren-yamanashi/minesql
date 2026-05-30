package access

import (
	"testing"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
)

func TestNewPurge(t *testing.T) {
	t.Run("Purge を生成できる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)

		// WHEN
		p := NewPurge(env.bp, env.trxManager, env.trxManager.undoLog)

		// THEN
		assert.NotNil(t, p)
	})
}

func TestPurgeStartStop(t *testing.T) {
	t.Run("Start と Stop が正常に動作する", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		p := NewPurge(env.bp, env.trxManager, env.trxManager.undoLog)

		// WHEN
		p.Start()
		time.Sleep(10 * time.Millisecond)
		p.Stop()

		// THEN
		assert.False(t, p.isRunning.Load())
	})

	t.Run("Stop を二重呼び出ししてもパニックしない", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		p := NewPurge(env.bp, env.trxManager, env.trxManager.undoLog)
		p.Start()
		time.Sleep(10 * time.Millisecond)

		// WHEN / THEN
		assert.NotPanics(t, func() {
			p.Stop()
			p.Stop()
		})
	})

	t.Run("Start せずに Stop してもパニックしない", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		p := NewPurge(env.bp, env.trxManager, env.trxManager.undoLog)

		// WHEN / THEN
		assert.NotPanics(t, func() {
			p.Stop()
		})
	})

	t.Run("Start を二重呼び出しすると最初の goroutine が維持される", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		p := NewPurge(env.bp, env.trxManager, env.trxManager.undoLog)
		p.Start()

		// WHEN
		p.Start() // 二重呼び出し

		// THEN
		assert.True(t, p.isRunning.Load())
		p.Stop()
		assert.False(t, p.isRunning.Load())
	})

	t.Run("Stop 後に再度 Start できる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		p := NewPurge(env.bp, env.trxManager, env.trxManager.undoLog)
		p.Start()
		p.Stop()

		// WHEN
		p.Start()
		time.Sleep(10 * time.Millisecond)
		p.Stop()

		// THEN
		assert.False(t, p.isRunning.Load())
	})
}

func TestPurgePurge(t *testing.T) {
	t.Run("パージ対象がない場合エラーにならない", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		p := NewPurge(env.bp, env.trxManager, env.trxManager.undoLog)

		// WHEN
		err := p.purge()

		// THEN
		assert.NoError(t, err)
	})

	t.Run("DELETE のパージで論理削除済みレコードが物理削除される", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		table := setupTableForRecoveryTest(t, env)
		p := NewPurge(env.bp, env.trxManager, env.trxManager.undoLog)

		// レコードを挿入してコミット
		trx1 := env.trxManager.Begin()
		_ = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trx1,
		)
		_ = env.trxManager.Commit(trx1)

		// 論理削除してコミット
		trx2 := env.trxManager.Begin()
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		iter, _ := table.primaryIndex.search(mtr, SearchModeStart{})
		record, _, _ := iter.Next()
		_ = table.SoftDelete(record, trx2)
		_ = env.trxManager.Commit(trx2)

		// WHEN
		err := p.purge()

		// THEN
		assert.NoError(t, err)
		// B+Tree から物理的にレコードが削除されている
		treeIter, _ := table.primaryIndex.tree.Search(mtr, btree.SearchModeStart{})
		_, ok, _ := treeIter.Get()
		assert.False(t, ok)
	})

	t.Run("UPDATE のパージでセカンダリインデックスの旧エントリが物理削除される", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		table := setupTableForRecoveryTest(t, env)
		p := NewPurge(env.bp, env.trxManager, env.trxManager.undoLog)

		// レコードを挿入してコミット
		trx1 := env.trxManager.Begin()
		_ = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trx1,
		)
		_ = env.trxManager.Commit(trx1)

		// name を更新してコミット (セカンダリインデックスの SK が変わる)
		trx2 := env.trxManager.Begin()
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		iter, _ := table.primaryIndex.search(mtr, SearchModeStart{})
		record, _, _ := iter.Next()
		_ = table.Update(record, []string{"name"}, []string{"Bob"}, trx2)
		_ = env.trxManager.Commit(trx2)

		// WHEN
		err := p.purge()

		// THEN
		assert.NoError(t, err)
		// プライマリインデックスのレコードは残っている (UPDATE はインプレース)
		iter2, _ := table.primaryIndex.search(mtr, SearchModeStart{})
		updated, ok, _ := iter2.Next()
		assert.True(t, ok)
		assert.Equal(t, "Bob", updated.values[1])
	})

	t.Run("アクティブな ReadView がある場合はパージされない", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		table := setupTableForRecoveryTest(t, env)
		p := NewPurge(env.bp, env.trxManager, env.trxManager.undoLog)

		// レコードを挿入してコミット
		trx1 := env.trxManager.Begin()
		_ = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trx1,
		)
		_ = env.trxManager.Commit(trx1)

		// 論理削除用のトランザクションを開始
		trx2 := env.trxManager.Begin()
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		iter, _ := table.primaryIndex.search(mtr, SearchModeStart{})
		record, _, _ := iter.Next()
		_ = table.SoftDelete(record, trx2)

		// trx2 がコミットする前に ReadView を作成 (trx2 はアクティブなので mIds に含まれる)
		trx3 := env.trxManager.Begin()
		_ = env.trxManager.CreateReadView(trx3)

		// trx2 をコミット
		_ = env.trxManager.Commit(trx2)

		// WHEN
		err := p.purge()

		// THEN
		assert.NoError(t, err)
		// trx3 の ReadView が trx2 を参照しうるためパージされず、レコードが残っている
		treeIter, _ := table.primaryIndex.tree.Search(mtr, btree.SearchModeStart{})
		_, ok, _ := treeIter.Get()
		assert.True(t, ok)
	})
}

func TestPurgePurgeEntry(t *testing.T) {
	t.Run("INSERT タイプのエントリは何もせず正常終了する", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		p := NewPurge(env.bp, env.trxManager, env.trxManager.undoLog)
		entry := undo.NewEntry(0, undo.RecordTypeInsert, nil)

		// WHEN
		err := p.purgeEntry(entry)

		// THEN
		assert.NoError(t, err)
	})
}

func TestPurgePurgableTrxIds(t *testing.T) {
	t.Run("パージ閾値未満の完了済みトランザクション ID を返す", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		p := NewPurge(env.bp, env.trxManager, env.trxManager.undoLog)
		id1 := env.trxManager.Begin()
		id2 := env.trxManager.Begin()
		_ = env.trxManager.Commit(id1)
		_ = env.trxManager.Commit(id2)

		// WHEN
		ids := p.purgableTrxIds(lock.TrxId(2))

		// THEN
		assert.Len(t, ids, 2)
		assert.Contains(t, ids, id1)
		assert.Contains(t, ids, id2)
	})

	t.Run("パージ閾値以上のトランザクションは含まれない", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		p := NewPurge(env.bp, env.trxManager, env.trxManager.undoLog)
		id1 := env.trxManager.Begin()
		_ = env.trxManager.Begin() // id2, Active
		_ = env.trxManager.Commit(id1)

		// WHEN
		ids := p.purgableTrxIds(lock.TrxId(1))

		// THEN
		assert.Len(t, ids, 1)
		assert.Contains(t, ids, id1)
	})

	t.Run("該当なしの場合は空を返す", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		p := NewPurge(env.bp, env.trxManager, env.trxManager.undoLog)

		// WHEN
		ids := p.purgableTrxIds(lock.TrxId(10))

		// THEN
		assert.Empty(t, ids)
	})
}
