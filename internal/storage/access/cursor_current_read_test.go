package access

import (
	"sync"
	"testing"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/stretchr/testify/assert"
)

func TestTableSearchForUpdate(t *testing.T) {
	t.Run("最新バージョンを排他ロック付きで取得できる", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx := tm.Begin()

		// WHEN
		rec, found, err := table.SearchForUpdate(trx, SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, rec.values)
		// 別トランザクションが同じ行に排他ロックを即時付与できない (= 排他ロックが取得済み)
		rowKey := lock.RowKey{MetaPageId: table.primaryIndex.tree.MetaPageId(), Key: rec.Encode().Key()}
		granted, _ := table.lock.TryLock(trx.trxId+1000, rowKey, lock.Exclusive)
		assert.False(t, granted)
	})

	t.Run("Read View を参照せず最新のコミット済みバージョンを読む", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		// reader が Consistent Read で Read View を作成し alice を読む
		reader := tm.Begin()
		snap := searchByPkForTrx(t, nil, table, reader, "1")
		assert.Equal(t, "Alice", snap.values[1])
		// 別トランザクションが Charlie に更新してコミットする
		writer := tm.Begin()
		wt := currentReadFirst(t, table, writer)
		assert.NoError(t, table.Update(writer, wt, []string{"name"}, []string{"Charlie"}))
		assert.NoError(t, tm.Commit(writer))

		// WHEN
		// reader の Consistent Read は Read View により alice のまま
		again := searchByPkForTrx(t, nil, table, reader, "1")
		// reader の Current Read は Read View を無視して最新の Charlie を読む
		cur, found, err := table.SearchForUpdate(reader, SearchModeStart{})

		// THEN
		assert.Equal(t, "Alice", again.values[1])
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "Charlie", cur.values[1])
	})

	t.Run("別トランザクションが更新・コミットしロックを解放した後の即時取得でも再検索で最新版を返す", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		// writer が排他ロックを取得 → Bob に更新 → コミット (= ロック解放)
		writer := tm.Begin()
		wt := currentReadFirst(t, table, writer)
		assert.NoError(t, table.Update(writer, wt, []string{"name"}, []string{"Bob"}))
		assert.NoError(t, tm.Commit(writer))

		// WHEN
		// writer は解放済みなのでロックは即時付与 (granted=true) される
		// ロック取得後に findByKey で再検索されるため、最新版 Bob が返らなければならない
		reader := tm.Begin()
		rec, found, err := table.SearchForUpdate(reader, SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "Bob", rec.values[1])
	})

	t.Run("最新バージョンが削除済みの場合は found=false", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx1 := tm.Begin()
		target := currentReadFirst(t, table, trx1)
		assert.NoError(t, table.SoftDelete(trx1, target))
		assert.NoError(t, tm.Commit(trx1))

		// WHEN
		trx2 := tm.Begin()
		_, found, err := table.SearchForUpdate(trx2, SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("対象キーが存在しない場合は found=false", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx := tm.Begin()

		// WHEN
		_, found, err := table.SearchForUpdate(trx, SearchModeKey{Key: [][]byte{[]byte("999")}})

		// THEN
		assert.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("ロック待ち後に再検索して最新バージョンを取得できる", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx1 := tm.Begin()
		_ = currentReadFirst(t, table, trx1) // trx1 が alice を排他ロック (未コミット)

		trx2 := tm.Begin()
		var wg sync.WaitGroup
		var rec *PrimaryRecord
		var found bool
		var waitErr error
		wg.Go(func() {
			rec, found, waitErr = table.SearchForUpdate(trx2, SearchModeStart{})
		})

		// WHEN
		time.Sleep(10 * time.Millisecond)
		assert.NoError(t, tm.Commit(trx1))
		wg.Wait()

		// THEN
		assert.NoError(t, waitErr)
		assert.True(t, found)
		assert.Equal(t, "Alice", rec.values[1])
	})

	t.Run("ロック待ち中に対象行が削除されると found=false", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx1 := tm.Begin()
		target := currentReadFirst(t, table, trx1)
		assert.NoError(t, table.SoftDelete(trx1, target)) // trx1 が alice を排他ロック + 論理削除 (未コミット)

		trx2 := tm.Begin()
		var wg sync.WaitGroup
		var found bool
		var waitErr error
		wg.Go(func() {
			_, found, waitErr = table.SearchForUpdate(trx2, SearchModeStart{})
		})

		// WHEN
		time.Sleep(10 * time.Millisecond)
		assert.NoError(t, tm.Commit(trx1))
		wg.Wait()

		// THEN
		assert.NoError(t, waitErr)
		assert.False(t, found)
	})
}

// currentReadFirst は UPDATE/DELETE 対象の先頭レコードを Current Read で取得する
func currentReadFirst(t *testing.T, table *Table, trx *Transaction) *PrimaryRecord {
	t.Helper()
	record, ok, err := table.SearchForUpdate(trx, SearchModeStart{})
	if err != nil {
		t.Fatalf("Current Read に失敗: %v", err)
	}
	if !ok {
		t.Fatal("対象レコードが見つからない")
	}
	return record
}

// currentReadByPk は指定 PK の対象レコードを Current Read で取得する (見つからなければ nil)
func currentReadByPk(t *testing.T, table *Table, trx *Transaction, pk string) *PrimaryRecord {
	t.Helper()
	record, ok, err := table.SearchForUpdate(trx, SearchModeKey{Key: [][]byte{[]byte(pk)}})
	if err != nil {
		t.Fatalf("Current Read に失敗: %v", err)
	}
	if !ok {
		return nil
	}
	return record
}
