package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/stretchr/testify/assert"
)

func TestMVCCRepeatableRead(t *testing.T) {
	t.Run("Read View 作成後の他トランザクションの UPDATE は不可視 (旧値が見える)", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		trx1 := env.trxMgr.Begin()
		_ = table.Insert([]string{"id", "name", "email"}, []string{"1", "alice", "alice@example.com"}, trx1)
		assert.NoError(t, env.trxMgr.Commit(trx1))

		// trx2 が Read View を作成 (1 回目の Search で確定)
		trx2 := env.trxMgr.Begin()
		v1 := searchByPkForTrx(t, env, table, trx2, "1")
		assert.Equal(t, "alice", v1.values[1])

		// trx3 が UPDATE してコミット
		trx3 := env.trxMgr.Begin()
		assert.NoError(t, table.Update(v1, []string{"name"}, []string{"bob"}, trx3))
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
		_ = table.Insert([]string{"id", "name", "email"}, []string{"1", "alice", "alice@example.com"}, trx1)
		assert.NoError(t, env.trxMgr.Commit(trx1))

		// trx2 を開始 (Read View はまだ未作成)
		trx2 := env.trxMgr.Begin()

		// trx3 が UPDATE (未コミット)
		trx3 := env.trxMgr.Begin()
		latest := searchByPkForTrx(t, env, table, trx3, "1") // trx3 自身では新値書き込み前に旧値が見える
		assert.NoError(t, table.Update(latest, []string{"name"}, []string{"bob"}, trx3))

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

		// trx1: INSERT v1
		trx1 := env.trxMgr.Begin()
		_ = table.Insert([]string{"id", "name", "email"}, []string{"1", "v1", "v1@example.com"}, trx1)
		assert.NoError(t, env.trxMgr.Commit(trx1))

		// trx2: UPDATE v1 → v2
		trx2 := env.trxMgr.Begin()
		v1 := searchByPkForTrx(t, env, table, trx2, "1")
		assert.NoError(t, table.Update(v1, []string{"name"}, []string{"v2"}, trx2))
		assert.NoError(t, env.trxMgr.Commit(trx2))

		// trxRead が Read View 作成 (v2 が見える状態)
		trxRead := env.trxMgr.Begin()
		atRead := searchByPkForTrx(t, env, table, trxRead, "1")
		assert.Equal(t, "v2", atRead.values[1])

		// trxLate: UPDATE v2 → v3 してコミット
		trxLate := env.trxMgr.Begin()
		latest := searchByPkForTrx(t, env, table, trxLate, "1")
		assert.NoError(t, table.Update(latest, []string{"name"}, []string{"v3"}, trxLate))
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

		// trx1 が INSERT (未コミット)
		trx1 := env.trxMgr.Begin()
		_ = table.Insert([]string{"id", "name", "email"}, []string{"1", "alice", "alice@example.com"}, trx1)

		// trx2 を開始
		trx2 := env.trxMgr.Begin()
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()

		// WHEN
		iter, err := table.Search(env.trxMgr, trx2, mtr, SearchModeStart{})
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
		_ = table.Insert([]string{"id", "name", "email"}, []string{"1", "alice", "alice@example.com"}, trx1)
		assert.NoError(t, env.trxMgr.Commit(trx1))

		// trx2 が Read View を作成
		trx2 := env.trxMgr.Begin()
		mtr2 := buffer.NewMtr(env.bp)
		defer mtr2.UnpinAll()
		iter2, err := table.SearchSecondary(env.trxMgr, trx2, mtr2, "idx_name", SearchModeStart{})
		assert.NoError(t, err)
		r1, ok, err := iter2.Next()
		iter2.Close()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "alice", r1.values[1])

		// trx3 が name を更新してコミット
		trx3 := env.trxMgr.Begin()
		latest := searchByPkForTrx(t, env, table, trx3, "1")
		assert.NoError(t, table.Update(latest, []string{"name"}, []string{"bob"}, trx3))
		assert.NoError(t, env.trxMgr.Commit(trx3))

		// WHEN
		mtrAgain := buffer.NewMtr(env.bp)
		defer mtrAgain.UnpinAll()
		iter3, err := table.SearchSecondary(env.trxMgr, trx2, mtrAgain, "idx_name", SearchModeStart{})
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
		_ = table.Insert([]string{"id", "name", "email"}, []string{"1", "Alice", "alice@example.com"}, t1)
		assert.NoError(t, env.trxMgr.Commit(t1))

		r1 := env.trxMgr.Begin()
		_ = env.trxMgr.CreateReadView(r1)

		t2 := env.trxMgr.Begin()
		latestForT2 := searchByPkForTrx(t, env, table, t2, "1")
		assert.NoError(t, table.Update(latestForT2, []string{"name"}, []string{"Bob"}, t2))
		assert.NoError(t, env.trxMgr.Commit(t2))

		r2 := env.trxMgr.Begin()
		_ = env.trxMgr.CreateReadView(r2)

		t3 := env.trxMgr.Begin()
		latestForT3 := searchByPkForTrx(t, env, table, t3, "1")
		assert.NoError(t, table.SoftDelete(latestForT3, t3))
		assert.NoError(t, env.trxMgr.Commit(t3))

		r3 := env.trxMgr.Begin()
		_ = env.trxMgr.CreateReadView(r3)

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
func collectIndexOnlyValues(t *testing.T, env *integrationEnv, table *Table, trxId lock.TrxId, indexName string) []string {
	t.Helper()
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	iter, err := table.SearchSecondary(env.trxMgr, trxId, mtr, indexName, SearchModeStart{})
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
		_ = table.Insert([]string{"id", "name", "email"}, []string{"1", "alice", "alice@example.com"}, trx1)
		assert.NoError(t, env.trxMgr.Commit(trx1))

		trx2 := env.trxMgr.Begin()
		latest := searchByPkForTrx(t, env, table, trx2, "1")
		assert.NoError(t, table.SoftDelete(latest, trx2))
		assert.NoError(t, env.trxMgr.Commit(trx2))

		// WHEN
		trx3 := env.trxMgr.Begin()
		v := searchByPkForTrx(t, env, table, trx3, "1")

		// THEN
		assert.Nil(t, v)
	})
}

// searchByPkForTrx は trxId 向けの ReadView で PK 検索を実行する
func searchByPkForTrx(t *testing.T, env *integrationEnv, table *Table, trxId lock.TrxId, pk string) *PrimaryRecord {
	t.Helper()
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	iter, err := table.Search(env.trxMgr, trxId, mtr, SearchModeKey{Key: [][]byte{[]byte(pk)}})
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
