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
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId(), env2.ddlMgr)
		assert.NoError(t, r.Execute())

		table2, err := NewTable(env2.bp, env2.ct, env2.undoLog, env2.lockMgr, env2.redoLog, "users")
		assert.NoError(t, err)
		p := NewPurge(env2.trxMgr)
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

	t.Run("再起動時に DELETE Undo を持つ trxId が InactiveTrxIds に再構築される", func(t *testing.T) {
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
		deleteTrxId := trx2.trxId
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId(), env2.ddlMgr)
		assert.NoError(t, r.Execute())

		// THEN
		ids := env2.trxMgr.InactiveTrxIds()
		assert.Contains(t, ids, deleteTrxId, "DELETE Undo を持つ trxId は再構築された InactiveTrxIds に含まれる")
		assert.NotContains(t, ids, trx1.trxId, "INSERT のみの trxId は History List から外れているため含まれない")
	})

	t.Run("Insert のみのコミット済みトランザクションは InactiveTrxIds に含まれない", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		flushBaseline(t, env)
		trx := env.trxMgr.Begin()
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)
		assert.NoError(t, env.trxMgr.Commit(trx))
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId(), env2.ddlMgr)
		assert.NoError(t, r.Execute())

		// THEN
		ids := env2.trxMgr.InactiveTrxIds()
		assert.NotContains(t, ids, trx.trxId, "INSERT のみのトランザクションは History List から外れる")
	})
}
