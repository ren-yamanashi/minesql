package access

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestCurrentReadIteratorNext(t *testing.T) {
	t.Run("最新バージョンを排他ロック付きで取得できる", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx := tm.Begin()

		// WHEN
		iter, err := table.SearchForUpdate(trx, SearchModeStart{})
		assert.NoError(t, err)
		defer iter.Close()
		rec, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, rec.values)
		rowKey := lock.RowKey{MetaPageId: table.primaryIndex.tree.MetaPageId(), Key: rec.Encode().Key()}
		granted, _ := table.lock.TryLock(trx.trxId+1000, rowKey, lock.Exclusive)
		assert.False(t, granted)
	})

	t.Run("Read View を参照せず最新のコミット済みバージョンを読む", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		reader := tm.Begin()
		snap := searchByPkForTrx(t, nil, table, reader, "1")
		assert.Equal(t, "Alice", snap.values[1])
		writer := tm.Begin()
		wt := currentReadFirst(t, table, writer)
		assert.NoError(t, table.Update(writer, wt, []string{"name"}, []string{"Charlie"}))
		assert.NoError(t, tm.Commit(writer))

		// WHEN
		again := searchByPkForTrx(t, nil, table, reader, "1")
		iter, err := table.SearchForUpdate(reader, SearchModeStart{})
		assert.NoError(t, err)
		defer iter.Close()
		cur, ok, err := iter.Next()

		// THEN
		assert.Equal(t, "Alice", again.values[1])
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Charlie", cur.values[1])
	})

	t.Run("対象キーが存在しない場合は ok=false", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx := tm.Begin()

		// WHEN
		iter, err := table.SearchForUpdate(trx, SearchModeKey{Key: [][]byte{[]byte("999")}})
		assert.NoError(t, err)
		defer iter.Close()
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("複数行を昇順に走査し各行に排他ロックを取得する", func(t *testing.T) {
		// GIVEN
		table, tm := setupTableWithRecords(t, [][]string{
			{"1", "Alice", "alice@example.com"},
			{"2", "Bob", "bob@example.com"},
			{"3", "Charlie", "charlie@example.com"},
		})
		trx := tm.Begin()

		// WHEN
		iter, err := table.SearchForUpdate(trx, SearchModeStart{})
		assert.NoError(t, err)
		defer iter.Close()
		var got [][]string
		for {
			rec, ok, err := iter.Next()
			assert.NoError(t, err)
			if !ok {
				break
			}
			got = append(got, rec.values)
			rowKey := lock.RowKey{MetaPageId: table.primaryIndex.tree.MetaPageId(), Key: rec.Encode().Key()}
			granted, _ := table.lock.TryLock(trx.trxId+1000, rowKey, lock.Exclusive)
			assert.False(t, granted)
		}

		// THEN
		assert.Equal(t, [][]string{
			{"1", "Alice", "alice@example.com"},
			{"2", "Bob", "bob@example.com"},
			{"3", "Charlie", "charlie@example.com"},
		}, got)
	})

	t.Run("最新バージョンが削除済みの行はスキップされロックは取得される", func(t *testing.T) {
		// GIVEN
		table, tm := setupTableWithRecords(t, [][]string{
			{"1", "Alice", "alice@example.com"},
			{"2", "Bob", "bob@example.com"},
		})
		deleter := tm.Begin()
		target := currentReadByPk(t, table, deleter, "1")
		assert.NoError(t, table.SoftDelete(deleter, target))
		assert.NoError(t, tm.Commit(deleter))
		trx := tm.Begin()

		// WHEN
		iter, err := table.SearchForUpdate(trx, SearchModeStart{})
		assert.NoError(t, err)
		defer iter.Close()
		var got [][]string
		for {
			rec, ok, err := iter.Next()
			assert.NoError(t, err)
			if !ok {
				break
			}
			got = append(got, rec.values)
		}

		// THEN
		assert.Equal(t, [][]string{{"2", "Bob", "bob@example.com"}}, got)
		rowKey := lock.RowKey{MetaPageId: table.primaryIndex.tree.MetaPageId(), Key: target.Encode().Key()}
		granted, _ := table.lock.TryLock(trx.trxId+1000, rowKey, lock.Exclusive)
		assert.False(t, granted)
	})

	t.Run("他トランザクションのロック保持解除後に更新後の最新版が返る", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		holder := tm.Begin()
		holderRec := currentReadFirst(t, table, holder)
		assert.NoError(t, table.Update(holder, holderRec, []string{"name"}, []string{"Bob"}))

		waiter := tm.Begin()
		var wg sync.WaitGroup
		var rec *PrimaryRecord
		var ok bool
		var waitErr error
		wg.Go(func() {
			iter, err := table.SearchForUpdate(waiter, SearchModeStart{})
			if err != nil {
				waitErr = err
				return
			}
			defer iter.Close()
			rec, ok, waitErr = iter.Next()
		})

		// WHEN
		time.Sleep(10 * time.Millisecond)
		assert.NoError(t, tm.Commit(holder))
		wg.Wait()

		// THEN
		assert.NoError(t, waitErr)
		assert.True(t, ok)
		assert.Equal(t, "Bob", rec.values[1])
	})

	t.Run("待機中に対象行が削除された場合はスキップして次行へ進む", func(t *testing.T) {
		// GIVEN
		table, tm := setupTableWithRecords(t, [][]string{
			{"1", "Alice", "alice@example.com"},
			{"2", "Bob", "bob@example.com"},
		})
		holder := tm.Begin()
		holderRec := currentReadByPk(t, table, holder, "1")
		assert.NoError(t, table.SoftDelete(holder, holderRec))

		waiter := tm.Begin()
		var wg sync.WaitGroup
		var got [][]string
		var waitErr error
		wg.Go(func() {
			iter, err := table.SearchForUpdate(waiter, SearchModeStart{})
			if err != nil {
				waitErr = err
				return
			}
			defer iter.Close()
			for {
				rec, ok, err := iter.Next()
				if err != nil {
					waitErr = err
					return
				}
				if !ok {
					return
				}
				got = append(got, rec.values)
			}
		})

		// WHEN
		time.Sleep(10 * time.Millisecond)
		assert.NoError(t, tm.Commit(holder))
		wg.Wait()

		// THEN
		assert.NoError(t, waitErr)
		assert.Equal(t, [][]string{{"2", "Bob", "bob@example.com"}}, got)
	})

	t.Run("待機中にリーフページの排他ラッチを別 mtr が即時取得できる", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		leafPageId := leafPageIdForKey(t, table, [][]byte{[]byte("1")})
		holder := tm.Begin()
		_ = currentReadFirst(t, table, holder)

		waiter := tm.Begin()
		var wg sync.WaitGroup
		var waitErr error
		wg.Go(func() {
			iter, err := table.SearchForUpdate(waiter, SearchModeStart{})
			if err != nil {
				waitErr = err
				return
			}
			defer iter.Close()
			_, _, waitErr = iter.Next()
		})

		time.Sleep(10 * time.Millisecond)

		// WHEN
		latched := make(chan struct{})
		go func() {
			mtr := buffer.NewMtr(table.bufferPool)
			defer mtr.UnpinAll()
			_, err := mtr.PageForWrite(leafPageId)
			assert.NoError(t, err)
			close(latched)
		}()

		// THEN
		select {
		case <-latched:
		case <-time.After(50 * time.Millisecond):
			t.Fatal("waiter が待機中にリーフの X ラッチを取得できない (走査側がラッチを保持したまま待機している)")
		}
		assert.NoError(t, tm.Commit(holder))
		wg.Wait()
		assert.NoError(t, waitErr)
	})

	t.Run("wait を挟んでも残りの行を全件読める", func(t *testing.T) {
		// GIVEN
		table, tm := setupTableWithRecords(t, [][]string{
			{"1", "Alice", "alice@example.com"},
			{"2", "Bob", "bob@example.com"},
			{"3", "Charlie", "charlie@example.com"},
		})
		holder := tm.Begin()
		_ = currentReadByPk(t, table, holder, "1")

		waiter := tm.Begin()
		var wg sync.WaitGroup
		var got [][]string
		var waitErr error
		wg.Go(func() {
			iter, err := table.SearchForUpdate(waiter, SearchModeStart{})
			if err != nil {
				waitErr = err
				return
			}
			defer iter.Close()
			for {
				rec, ok, err := iter.Next()
				if err != nil {
					waitErr = err
					return
				}
				if !ok {
					return
				}
				got = append(got, rec.values)
			}
		})

		// WHEN
		time.Sleep(10 * time.Millisecond)
		assert.NoError(t, tm.Commit(holder))
		wg.Wait()

		// THEN
		assert.NoError(t, waitErr)
		assert.Equal(t, [][]string{
			{"1", "Alice", "alice@example.com"},
			{"2", "Bob", "bob@example.com"},
			{"3", "Charlie", "charlie@example.com"},
		}, got)
	})

	t.Run("ロック待機がタイムアウトするとエラーが返る", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		holder := tm.Begin()
		_ = currentReadFirst(t, table, holder)

		// WHEN
		waiter := tm.Begin()
		iter, err := table.SearchForUpdate(waiter, SearchModeStart{})
		assert.NoError(t, err)
		defer iter.Close()
		_, _, err = iter.Next()

		// THEN
		assert.ErrorIs(t, err, lock.ErrTimeout)
	})
}

func TestCurrentReadIteratorClose(t *testing.T) {
	t.Run("途中打ち切りと再 Close で panic せず後続の走査に影響しない", func(t *testing.T) {
		// GIVEN
		table, tm := setupTableWithRecords(t, [][]string{
			{"1", "Alice", "alice@example.com"},
			{"2", "Bob", "bob@example.com"},
		})
		trx := tm.Begin()

		// WHEN
		iter, err := table.SearchForUpdate(trx, SearchModeStart{})
		assert.NoError(t, err)
		rec, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "1", rec.values[0])
		iter.Close()
		iter.Close()

		// THEN
		iter2, err := table.SearchForUpdate(trx, SearchModeStart{})
		assert.NoError(t, err)
		defer iter2.Close()
		var got [][]string
		for {
			r, ok, err := iter2.Next()
			assert.NoError(t, err)
			if !ok {
				break
			}
			got = append(got, r.values)
		}
		assert.Equal(t, [][]string{
			{"1", "Alice", "alice@example.com"},
			{"2", "Bob", "bob@example.com"},
		}, got)
	})
}

// currentReadFirst は Current Read で先頭レコードを取得する
func currentReadFirst(t *testing.T, table *Table, trx *Transaction) *PrimaryRecord {
	t.Helper()
	iter, err := table.SearchForUpdate(trx, SearchModeStart{})
	if err != nil {
		t.Fatalf("SearchForUpdate に失敗: %v", err)
	}
	defer iter.Close()
	record, ok, err := iter.Next()
	if err != nil {
		t.Fatalf("Current Read に失敗: %v", err)
	}
	if !ok {
		t.Fatal("対象レコードが見つからない")
	}
	return record
}

// currentReadByPk は指定 PK の Current Read を実行する (完全一致しない場合は nil を返す)
func currentReadByPk(t *testing.T, table *Table, trx *Transaction, pk string) *PrimaryRecord {
	t.Helper()
	mode := SearchModeKey{Key: [][]byte{[]byte(pk)}}
	iter, err := table.SearchForUpdate(trx, mode)
	if err != nil {
		t.Fatalf("SearchForUpdate に失敗: %v", err)
	}
	defer iter.Close()
	record, ok, err := iter.Next()
	if err != nil {
		t.Fatalf("Current Read に失敗: %v", err)
	}
	if !ok {
		return nil
	}
	wantEncoded := mode.Encode().(btree.SearchModeKey).Key
	if !bytes.Equal(record.Encode().Key(), wantEncoded) {
		return nil
	}
	return record
}

// setupTableWithRecords は指定した複数レコードを挿入した状態の Table と TrxManager を返す
func setupTableWithRecords(t *testing.T, rows [][]string) (*Table, *TrxManager) {
	t.Helper()
	env := setupTableTestEnv(t)
	table, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, env.redoLog, "users")
	if err != nil {
		t.Fatalf("Table の作成に失敗: %v", err)
	}
	trx := env.trxMgr.Begin()
	for _, row := range rows {
		if err := table.Insert(trx, []string{"id", "name", "email"}, row); err != nil {
			t.Fatalf("Insert に失敗: %v", err)
		}
	}
	if err := env.trxMgr.Commit(trx); err != nil {
		t.Fatalf("Commit に失敗: %v", err)
	}
	return table, env.trxMgr
}

// searchForUpdateForKey は指定 mode で Current Read の先頭 1 件を取得する
//   - Next 1 回 + Close を包み、単行取得の (record, found, err) 形で返す
func searchForUpdateForKey(table *Table, trx *Transaction, mode SearchMode) (*PrimaryRecord, bool, error) {
	iter, err := table.SearchForUpdate(trx, mode)
	if err != nil {
		return nil, false, err
	}
	defer iter.Close()
	return iter.Next()
}

// leafPageIdForKey は指定キーが属するリーフページの PageId を返す
func leafPageIdForKey(t *testing.T, table *Table, key [][]byte) page.Id {
	t.Helper()
	mtr := buffer.NewMtr(table.bufferPool)
	defer mtr.UnpinAll()
	iter, err := table.primaryIndex.tree.Search(mtr, SearchModeKey{Key: key}.Encode())
	if err != nil {
		t.Fatalf("Search に失敗: %v", err)
	}
	return iter.BufferPageId()
}
