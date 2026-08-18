package access

import (
	"strings"
	"testing"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPurge(t *testing.T) {
	t.Run("Purge を生成できる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)

		// WHEN
		p := NewPurge(env.trxManager)

		// THEN
		assert.NotNil(t, p)
	})
}

func TestPurgeStartStop(t *testing.T) {
	t.Run("Start と Stop が正常に動作する", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		p := NewPurge(env.trxManager)

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
		p := NewPurge(env.trxManager)
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
		p := NewPurge(env.trxManager)

		// WHEN / THEN
		assert.NotPanics(t, func() {
			p.Stop()
		})
	})

	t.Run("Start を二重呼び出しすると最初の goroutine が維持される", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		p := NewPurge(env.trxManager)
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
		p := NewPurge(env.trxManager)
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
		p := NewPurge(env.trxManager)

		// WHEN
		err := p.purge()

		// THEN
		assert.NoError(t, err)
	})

	t.Run("DELETE のパージで論理削除済みレコードが物理削除される", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		table := setupTableForRecoveryTest(t, env)
		p := NewPurge(env.trxManager)

		trx1 := env.trxManager.Begin()
		_ = table.Insert(
			trx1,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		_ = env.trxManager.Commit(trx1)

		trx2 := env.trxManager.Begin()
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		record := currentReadFirst(t, table, trx2)
		_ = table.SoftDelete(trx2, record)
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

	t.Run("論理削除後に同一キーで再挿入された場合はパージで物理削除されない", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		table := setupTableForRecoveryTest(t, env)
		p := NewPurge(env.trxManager)

		trx1 := env.trxManager.Begin()
		_ = table.Insert(
			trx1,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		_ = env.trxManager.Commit(trx1)

		trx2 := env.trxManager.Begin()
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		record := currentReadFirst(t, table, trx2)
		_ = table.SoftDelete(trx2, record)
		_ = env.trxManager.Commit(trx2)

		trx3 := env.trxManager.Begin()
		_ = table.Insert(
			trx3,
			[]string{"id", "name", "email"},
			[]string{"1", "Charlie", "charlie@example.com"},
		)
		_ = env.trxManager.Commit(trx3)

		// WHEN
		err := p.purge()

		// THEN
		assert.NoError(t, err)
		iter2, _ := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		reinserted, ok, _ := iter2.Next()
		assert.True(t, ok)
		assert.Equal(t, "Charlie", reinserted.values[1])
	})

	t.Run("UPDATE のパージでセカンダリインデックスの旧エントリが物理削除される", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		table := setupTableForRecoveryTest(t, env)
		p := NewPurge(env.trxManager)

		trx1 := env.trxManager.Begin()
		_ = table.Insert(
			trx1,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		_ = env.trxManager.Commit(trx1)

		trx2 := env.trxManager.Begin()
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		record := currentReadFirst(t, table, trx2)
		_ = table.Update(trx2, record, []string{"name"}, []string{"Bob"})
		_ = env.trxManager.Commit(trx2)

		// WHEN
		err := p.purge()

		// THEN
		assert.NoError(t, err)
		// プライマリインデックスのレコードは残っている (UPDATE はインプレース)
		iter2, _ := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		updated, ok, _ := iter2.Next()
		assert.True(t, ok)
		assert.Equal(t, "Bob", updated.values[1])
	})

	t.Run("アクティブな ReadView がある場合はパージされない", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		table := setupTableForRecoveryTest(t, env)
		p := NewPurge(env.trxManager)

		trx1 := env.trxManager.Begin()
		_ = table.Insert(
			trx1,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		_ = env.trxManager.Commit(trx1)

		trx2 := env.trxManager.Begin()
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		record := currentReadFirst(t, table, trx2)
		_ = table.SoftDelete(trx2, record)

		trx3 := env.trxManager.Begin()
		_ = env.trxManager.EnsureReadView(trx3)

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

	t.Run("purge 経由の merge で解放されたページが後続の AllocatePage で再利用される", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		table := setupTableForRecoveryTest(t, env)
		p := NewPurge(env.trxManager)

		longSuffix := strings.Repeat("a", 1500)
		trx1 := env.trxManager.Begin()
		for _, id := range []string{"1", "2", "3"} {
			require.NoError(t, table.Insert(
				trx1,
				[]string{"id", "name", "email"},
				[]string{id, "u" + id, id + "-" + longSuffix + "@example.com"},
			))
		}
		require.NoError(t, env.trxManager.Commit(trx1))

		pageIdsBefore := collectTreePageIds(t, env.bp, table.primaryIndex.tree)

		trx2 := env.trxManager.Begin()
		for _, id := range []string{"1", "2", "3"} {
			record := currentReadByPk(t, table, trx2, id)
			require.NotNil(t, record)
			require.NoError(t, table.SoftDelete(trx2, record))
		}
		require.NoError(t, env.trxManager.Commit(trx2))

		// WHEN
		err := p.purge()

		// THEN
		require.NoError(t, err)
		pageIdsAfter := collectTreePageIds(t, env.bp, table.primaryIndex.tree)
		assert.Less(t, len(pageIdsAfter), len(pageIdsBefore))

		freed := diffPageIds(pageIdsBefore, pageIdsAfter)
		require.NotEmpty(t, freed)
		assertAllPagesFree(t, env.bp, freed)

		minFreed := minPageId(freed)
		fileId := table.primaryIndex.fileId()
		allocMtr := buffer.NewWriteMtr(env.bp, lock.SystemReservedTrxId, env.redoLog)
		allocated, err := fsp.AllocatePage(allocMtr, fileId)
		require.NoError(t, err)
		require.NoError(t, allocMtr.Commit())
		assert.Equal(t, minFreed, allocated)
	})
}

// diffPageIds は before に含まれ after に含まれない PageId を返す
func diffPageIds(before, after []page.Id) []page.Id {
	afterSet := make(map[page.Id]struct{}, len(after))
	for _, pid := range after {
		afterSet[pid] = struct{}{}
	}
	var diff []page.Id
	for _, pid := range before {
		if _, ok := afterSet[pid]; !ok {
			diff = append(diff, pid)
		}
	}
	return diff
}

// minPageId は PageNumber が最小の PageId を返す
func minPageId(pageIds []page.Id) page.Id {
	minId := pageIds[0]
	for _, pid := range pageIds[1:] {
		if pid.PageNumber() < minId.PageNumber() {
			minId = pid
		}
	}
	return minId
}

func TestPurgePurgeEntry(t *testing.T) {
	t.Run("INSERT タイプのエントリは何もせず正常終了する", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		p := NewPurge(env.trxManager)
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
		p := NewPurge(env.trxManager)
		trx1 := env.trxManager.Begin()
		trx2 := env.trxManager.Begin()
		_ = env.trxManager.Commit(trx1)
		_ = env.trxManager.Commit(trx2)

		// WHEN
		ids := p.purgableTrxIds(lock.TrxId(3))

		// THEN
		assert.Len(t, ids, 2)
		assert.Contains(t, ids, trx1.trxId)
		assert.Contains(t, ids, trx2.trxId)
	})

	t.Run("パージ閾値以上のトランザクションは含まれない", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		p := NewPurge(env.trxManager)
		trx1 := env.trxManager.Begin()
		_ = env.trxManager.Begin()
		_ = env.trxManager.Commit(trx1)

		// WHEN
		ids := p.purgableTrxIds(lock.TrxId(2))

		// THEN
		assert.Len(t, ids, 1)
		assert.Contains(t, ids, trx1.trxId)
	})

	t.Run("該当なしの場合は空を返す", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		p := NewPurge(env.trxManager)

		// WHEN
		ids := p.purgableTrxIds(lock.TrxId(10))

		// THEN
		assert.Empty(t, ids)
	})
}
