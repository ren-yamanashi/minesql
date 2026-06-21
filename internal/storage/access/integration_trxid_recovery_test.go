package access

import (
	"fmt"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/stretchr/testify/assert"
)

func TestIntegrationTrxIdRecovery(t *testing.T) {
	t.Run("再起動後の Begin は復元された最大 trxId+1 から払い出される", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		flushBaseline(t, env)
		var lastTrxId lock.TrxId
		for i := 1; i <= 5; i++ {
			trx := env.trxMgr.Begin()
			lastTrxId = trx.trxId
			err := table.Insert(
				trx,
				[]string{"id", "name", "email"},
				[]string{fmt.Sprintf("%d", i), fmt.Sprintf("user%d", i), fmt.Sprintf("u%d@example.com", i)},
			)
			assert.NoError(t, err)
			assert.NoError(t, env.trxMgr.Commit(trx))
		}
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId(), env2.ct.DDLManager())
		assert.NoError(t, r.Execute())

		// THEN
		trxNext := env2.trxMgr.Begin()
		assert.Greater(t, trxNext.trxId, lastTrxId)
	})

	t.Run("再起動後の ReadView が再起動前のコミット済みレコードを可視と判定する", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		flushBaseline(t, env)
		for i := 1; i <= 5; i++ {
			trx := env.trxMgr.Begin()
			err := table.Insert(
				trx,
				[]string{"id", "name", "email"},
				[]string{fmt.Sprintf("%d", i), fmt.Sprintf("user%d", i), fmt.Sprintf("u%d@example.com", i)},
			)
			assert.NoError(t, err)
			assert.NoError(t, env.trxMgr.Commit(trx))
		}
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId(), env2.ct.DDLManager())
		assert.NoError(t, r.Execute())

		table2, err := NewTable(env2.bp, env2.ct, env2.undoLog, env2.lockMgr, env2.redoLog, "users")
		assert.NoError(t, err)

		trxNext := env2.trxMgr.Begin()

		// THEN
		iter, err := table2.Search(trxNext, SearchModeStart{})
		assert.NoError(t, err)
		visible := 0
		for {
			_, ok, err := iter.Next()
			assert.NoError(t, err)
			if !ok {
				break
			}
			visible++
		}
		assert.Equal(t, 5, visible, "再起動前のコミット済みレコードが ReadView から可視")
	})
}
