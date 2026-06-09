package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/stretchr/testify/assert"
)

func TestMVCCRepeatableRead(t *testing.T) {
	t.Run("Read View 作成後の他トランザクションの UPDATE は不可視 (旧値が見える)", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		trx1 := env.trxMgr.Begin()
		_ = table.Insert(trx1, []string{"id", "name", "email"}, []string{"1", "alice", "alice@example.com"})
		assert.NoError(t, env.trxMgr.Commit(trx1))

		trx2 := env.trxMgr.Begin()
		v1 := searchByPkForTrx(t, env, table, trx2, "1")
		assert.Equal(t, "alice", v1.values[1])

		trx3 := env.trxMgr.Begin()
		assert.NoError(t, table.Update(trx3, v1, []string{"name"}, []string{"bob"}))
		assert.NoError(t, env.trxMgr.Commit(trx3))

		// WHEN
		v2 := searchByPkForTrx(t, env, table, trx2, "1")

		// THEN
		assert.NotNil(t, v2)
		assert.Equal(t, "alice", v2.values[1])
		assert.NoError(t, env.trxMgr.Commit(trx2))
		trx4 := env.trxMgr.Begin()
		v3 := searchByPkForTrx(t, env, table, trx4, "1")
		assert.Equal(t, "bob", v3.values[1])
	})
}

func TestMVCCUncommittedInvisible(t *testing.T) {
	t.Run("他トランザクションの未コミット UPDATE は不可視", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		trx1 := env.trxMgr.Begin()
		_ = table.Insert(trx1, []string{"id", "name", "email"}, []string{"1", "alice", "alice@example.com"})
		assert.NoError(t, env.trxMgr.Commit(trx1))

		trx2 := env.trxMgr.Begin()

		trx3 := env.trxMgr.Begin()
		latest := searchByPkForTrx(t, env, table, trx3, "1")
		assert.NoError(t, table.Update(trx3, latest, []string{"name"}, []string{"bob"}))

		// WHEN
		v := searchByPkForTrx(t, env, table, trx2, "1")

		// THEN
		assert.NotNil(t, v)
		assert.Equal(t, "alice", v.values[1])
	})
}

func TestMVCCMultiStepUndoTraversal(t *testing.T) {
	t.Run("複数段の UPDATE を遡って Read View 作成時点のバージョンが見える", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trx1 := env.trxMgr.Begin()
		_ = table.Insert(trx1, []string{"id", "name", "email"}, []string{"1", "v1", "v1@example.com"})
		assert.NoError(t, env.trxMgr.Commit(trx1))

		trx2 := env.trxMgr.Begin()
		v1 := searchByPkForTrx(t, env, table, trx2, "1")
		assert.NoError(t, table.Update(trx2, v1, []string{"name"}, []string{"v2"}))
		assert.NoError(t, env.trxMgr.Commit(trx2))

		trxRead := env.trxMgr.Begin()
		atRead := searchByPkForTrx(t, env, table, trxRead, "1")
		assert.Equal(t, "v2", atRead.values[1])

		trxLate := env.trxMgr.Begin()
		latest := searchByPkForTrx(t, env, table, trxLate, "1")
		assert.NoError(t, table.Update(trxLate, latest, []string{"name"}, []string{"v3"}))
		assert.NoError(t, env.trxMgr.Commit(trxLate))

		// WHEN
		v := searchByPkForTrx(t, env, table, trxRead, "1")

		// THEN
		assert.NotNil(t, v)
		assert.Equal(t, "v2", v.values[1])
	})
}

func TestMVCCInsertChainTerminal(t *testing.T) {
	t.Run("他トランザクションの未コミット INSERT はチェーン終端で空", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trx1 := env.trxMgr.Begin()
		_ = table.Insert(trx1, []string{"id", "name", "email"}, []string{"1", "alice", "alice@example.com"})

		trx2 := env.trxMgr.Begin()
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()

		// WHEN
		iter, err := table.Search(trx2, mtr, SearchModeStart{})
		assert.NoError(t, err)
		defer iter.Close()
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestMVCCSecondaryVisibility(t *testing.T) {
	t.Run("セカンダリ経由でも Read View 保持で旧値が見える", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trx1 := env.trxMgr.Begin()
		_ = table.Insert(trx1, []string{"id", "name", "email"}, []string{"1", "alice", "alice@example.com"})
		assert.NoError(t, env.trxMgr.Commit(trx1))

		trx2 := env.trxMgr.Begin()
		mtr2 := buffer.NewMtr(env.bp)
		defer mtr2.UnpinAll()
		iter2, err := table.SearchSecondary(trx2, mtr2, "idx_name", SearchModeStart{})
		assert.NoError(t, err)
		r1, ok, err := iter2.Next()
		iter2.Close()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "alice", r1.values[1])

		trx3 := env.trxMgr.Begin()
		latest := searchByPkForTrx(t, env, table, trx3, "1")
		assert.NoError(t, table.Update(trx3, latest, []string{"name"}, []string{"bob"}))
		assert.NoError(t, env.trxMgr.Commit(trx3))

		// WHEN
		mtrAgain := buffer.NewMtr(env.bp)
		defer mtrAgain.UnpinAll()
		iter3, err := table.SearchSecondary(trx2, mtrAgain, "idx_name", SearchModeStart{})
		assert.NoError(t, err)
		defer iter3.Close()
		result, _, err := iter3.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "alice", result.values[1])
	})
}

func TestMVCCSecondaryIndexOnlyComplexScenario(t *testing.T) {
	t.Run("INSERT → SK 変更 UPDATE → DELETE のシナリオで各時点の ReadView から NextIndexOnly が正しい結果を返す", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		t1 := env.trxMgr.Begin()
		_ = table.Insert(t1, []string{"id", "name", "email"}, []string{"1", "Alice", "alice@example.com"})
		assert.NoError(t, env.trxMgr.Commit(t1))

		r1 := env.trxMgr.Begin()
		_ = env.trxMgr.EnsureReadView(r1)

		t2 := env.trxMgr.Begin()
		latestForT2 := searchByPkForTrx(t, env, table, t2, "1")
		assert.NoError(t, table.Update(t2, latestForT2, []string{"name"}, []string{"Bob"}))
		assert.NoError(t, env.trxMgr.Commit(t2))

		r2 := env.trxMgr.Begin()
		_ = env.trxMgr.EnsureReadView(r2)

		t3 := env.trxMgr.Begin()
		latestForT3 := searchByPkForTrx(t, env, table, t3, "1")
		assert.NoError(t, table.SoftDelete(t3, latestForT3))
		assert.NoError(t, env.trxMgr.Commit(t3))

		r3 := env.trxMgr.Begin()
		_ = env.trxMgr.EnsureReadView(r3)

		// WHEN
		// THEN
		r1Values := collectIndexOnlyValues(t, env, table, r1, "idx_name")
		assert.Equal(t, []string{"Alice"}, r1Values)

		r2Values := collectIndexOnlyValues(t, env, table, r2, "idx_name")
		assert.Equal(t, []string{"Bob"}, r2Values)

		r3Values := collectIndexOnlyValues(t, env, table, r3, "idx_name")
		assert.Empty(t, r3Values)
	})
}

// collectIndexOnlyValues は SearchSecondary 経由で NextIndexOnly を全件呼んで SK 先頭値のリストを返す
func collectIndexOnlyValues(t *testing.T, env *integrationEnv, table *Table, trx *Transaction, indexName string) []string {
	t.Helper()
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	iter, err := table.SearchSecondary(trx, mtr, indexName, SearchModeStart{})
	if err != nil {
		t.Fatalf("SearchSecondary に失敗: %v", err)
	}
	defer iter.Close()

	var values []string
	for {
		rec, ok, err := iter.NextIndexOnly()
		if err != nil {
			t.Fatalf("NextIndexOnly に失敗: %v", err)
		}
		if !ok {
			break
		}
		values = append(values, rec.values[0])
	}
	return values
}

func TestMVCCDeletedVisible(t *testing.T) {
	t.Run("自トランザクションでコミット済みの DELETE は空", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		trx1 := env.trxMgr.Begin()
		_ = table.Insert(trx1, []string{"id", "name", "email"}, []string{"1", "alice", "alice@example.com"})
		assert.NoError(t, env.trxMgr.Commit(trx1))

		trx2 := env.trxMgr.Begin()
		latest := searchByPkForTrx(t, env, table, trx2, "1")
		assert.NoError(t, table.SoftDelete(trx2, latest))
		assert.NoError(t, env.trxMgr.Commit(trx2))

		// WHEN
		trx3 := env.trxMgr.Begin()
		v := searchByPkForTrx(t, env, table, trx3, "1")

		// THEN
		assert.Nil(t, v)
	})
}

// searchByPkForTrx は trx 向けの ReadView で PK 検索を実行する
func searchByPkForTrx(t *testing.T, env *integrationEnv, table *Table, trx *Transaction, pk string) *PrimaryRecord {
	t.Helper()
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	iter, err := table.Search(trx, mtr, SearchModeKey{Key: [][]byte{[]byte(pk)}})
	if err != nil {
		t.Fatalf("Search に失敗: %v", err)
	}
	defer iter.Close()
	rec, ok, err := iter.Next()
	if err != nil {
		t.Fatalf("Next に失敗: %v", err)
	}
	if !ok {
		return nil
	}
	return rec
}
