package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/stretchr/testify/assert"
)

func TestIntegrationUndoRecoveryPurge(t *testing.T) {
	t.Run("再起動 → Recovery → Purge で再起動前の deleteMark レコードが物理削除される", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		flushBaseline(t, env)

		trx1 := env.trxMgr.Begin()
		err := table.Insert(
			trx1,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)
		assert.NoError(t, env.trxMgr.Commit(trx1))

		trx2 := env.trxMgr.Begin()
		record := currentReadFirst(t, table, trx2)
		assert.NoError(t, table.SoftDelete(trx2, record))
		assert.NoError(t, env.trxMgr.Commit(trx2))
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId())
		assert.NoError(t, r.Execute())

		table2, err := NewTable(env2.bp, env2.ct, env2.undoLog, env2.lockMgr, env2.redoLog, "users")
		assert.NoError(t, err)
		p := NewPurge(env2.bp, env2.trxMgr, env2.undoLog, env2.redoLog)
		assert.NoError(t, p.purge())

		// THEN
		mtr := buffer.NewMtr(env2.bp)
		defer mtr.UnpinAll()
		iter, err := table2.primaryIndex.tree.Search(mtr, btree.SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Get()
		assert.NoError(t, err)
		assert.False(t, ok, "Purge により再起動前の deleteMark レコードが物理削除されている")
	})
}
