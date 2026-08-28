package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/stretchr/testify/assert"
)

func TestTrxManagerRollbackToSavepoint(t *testing.T) {
	t.Run("savepoint 以降の Insert のみ取り消され savepoint 以前の Insert は残る", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		assert.NoError(t, table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		))
		savepoint := trx.Savepoint()
		assert.NoError(t, table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "bob@example.com"},
		))

		// WHEN
		err := tm.RollbackToSavepoint(trx, savepoint)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, savepoint, trx.Savepoint())
		iter, err := table.primaryIndex.search(SearchModeStart{}, nil)
		assert.NoError(t, err)
		var keys []string
		for {
			rec, ok, err := iter.Next()
			assert.NoError(t, err)
			if !ok {
				break
			}
			keys = append(keys, rec.values[0])
		}
		assert.Equal(t, []string{"1"}, keys)
	})

	t.Run("savepoint = 0 は Rollback (全体) と同じ効果になる (ただしロックは解放しない)", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		assert.NoError(t, table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		))

		// WHEN
		err := tm.RollbackToSavepoint(trx, 0)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 0, trx.Savepoint())
		assert.Equal(t, trxStateActive, tm.transactions[trx.trxId].state)
		iter, err := table.primaryIndex.search(SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("savepoint が現在位置と同じ場合は no-op", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		assert.NoError(t, table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		))
		savepoint := trx.Savepoint()

		// WHEN
		err := tm.RollbackToSavepoint(trx, savepoint)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, savepoint, trx.Savepoint())
	})

	t.Run("RollbackToSavepoint はロックを解放しない (トランザクション継続)", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		assert.NoError(t, table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		))
		savepoint := trx.Savepoint()
		assert.NoError(t, table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "bob@example.com"},
		))
		assert.NoError(t, tm.RollbackToSavepoint(trx, savepoint))

		// WHEN: savepoint 以前に取得したロックが保持されているため、他トランザクションからは Insert 中の PK が触れない
		otherTrx := tm.Begin()
		otherErr := table.Insert(
			otherTrx,
			[]string{"id", "name", "email"},
			[]string{"1", "Charlie", "charlie@example.com"},
		)

		// THEN
		assert.Error(t, otherErr, "trx のロックが保持されているため PK=1 の Insert は失敗する")
	})

	t.Run("savepoint 以降を取り消した後もトランザクションは継続 (後続 Insert 可能)", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		assert.NoError(t, table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		))
		savepoint := trx.Savepoint()
		assert.NoError(t, table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "bob@example.com"},
		))

		// WHEN
		assert.NoError(t, tm.RollbackToSavepoint(trx, savepoint))
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"3", "Carol", "carol@example.com"},
		)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, trxStateActive, tm.transactions[trx.trxId].state)
	})

	t.Run("Unique セカンダリの dup key で部分挿入された行が RollbackToSavepoint で 3 ツリーから消える", func(t *testing.T) {
		// GIVEN: idx_email を Unique として持つ users テーブルを CreateTable で構築する
		// (setupTrxManager 経由の fixture は idx_name / idx_email が同一 B+Tree を共有する pre-existing 制約があるため使わない)
		env := setupCreateTableTestEnv(t)
		table, err := CreateTable(env.trxMgr, CreateTableInput{
			TableName: "users",
			ColNames:  []string{"id", "name", "email"},
			PkCount:   1,
			Indexes: []CreateIndexInput{
				{IndexName: "idx_name", ColNames: []string{"name"}, IndexType: dictionary.IndexTypeNonUnique},
				{IndexName: "idx_email", ColNames: []string{"email"}, IndexType: dictionary.IndexTypeUnique},
			},
		})
		assert.NoError(t, err)
		trx := env.trxMgr.Begin()
		assert.NoError(t, table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "a@example.com"},
		))
		savepoint := trx.Savepoint()
		dupErr := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "a@example.com"},
		)
		assert.ErrorIs(t, dupErr, btree.ErrDuplicateKey, "前提: idx_email の unique 制約違反で error が返る")

		// WHEN
		assert.NoError(t, env.trxMgr.RollbackToSavepoint(trx, savepoint))

		// THEN
		primaryIter, err := table.primaryIndex.search(SearchModeStart{}, nil)
		assert.NoError(t, err)
		var primaryKeys []string
		for {
			rec, ok, iterErr := primaryIter.Next()
			assert.NoError(t, iterErr)
			if !ok {
				break
			}
			primaryKeys = append(primaryKeys, rec.values[0])
		}

		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.search(SearchModeStart{}, nil)
		assert.NoError(t, err)
		type nameEntry struct{ sk, pk string }
		var nameEntries []nameEntry
		for {
			rec, ok, iterErr := nameIter.NextIndexOnly()
			assert.NoError(t, iterErr)
			if !ok {
				break
			}
			nameEntries = append(nameEntries, nameEntry{sk: rec.values[0], pk: rec.pk[0]})
		}

		idxEmail := findSecondaryIndex(t, table, "idx_email")
		emailIter, err := idxEmail.search(SearchModeStart{}, nil)
		assert.NoError(t, err)
		type emailEntry struct{ sk, pk string }
		var emailEntries []emailEntry
		for {
			rec, ok, iterErr := emailIter.NextIndexOnly()
			assert.NoError(t, iterErr)
			if !ok {
				break
			}
			emailEntries = append(emailEntries, emailEntry{sk: rec.values[0], pk: rec.pk[0]})
		}

		assert.Equal(t, []string{"1"}, primaryKeys, "プライマリに PK=2 の残骸は残らない")
		assert.Equal(t, []nameEntry{{sk: "Alice", pk: "1"}}, nameEntries, "idx_name に Bob+PK=2 の残骸は残らない")
		assert.Equal(t, []emailEntry{{sk: "a@example.com", pk: "1"}}, emailEntries, "idx_email に PK=2 の重複エントリは残らない")

		// WHEN: 後続 Insert が成功しトランザクションが継続する
		err = table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"3", "Carol", "c@example.com"},
		)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, trxStateActive, env.trxMgr.transactions[trx.trxId].state)
	})

	t.Run("同一 Undo 列への二重逆適用が成功し最終状態が変わらない", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		assert.NoError(t, table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		))
		savepoint := trx.Savepoint()
		assert.NoError(t, table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "bob@example.com"},
		))

		// WHEN: savepoint 以降を逆適用した後、Undo 列を差し戻してもう一度同じ範囲を逆適用する
		records := tm.undoLog.RecordsFrom(trx.trxId, savepoint)
		assert.Len(t, records, 1)
		assert.NoError(t, tm.RollbackToSavepoint(trx, savepoint))

		reappliedMtr := buffer.NewWriteMtr(tm.bufferPool, trx.trxId, tm.redoLog)
		reapplyErr := tm.rollbackRecord(reappliedMtr, records[0])
		commitErr := reappliedMtr.Commit()

		// THEN
		assert.NoError(t, reapplyErr, "既に取り消し済みの Insert への再逆適用は成功する")
		assert.NoError(t, commitErr)
		iter, err := table.primaryIndex.search(SearchModeStart{}, nil)
		assert.NoError(t, err)
		var keys []string
		for {
			rec, ok, err := iter.Next()
			assert.NoError(t, err)
			if !ok {
				break
			}
			keys = append(keys, rec.values[0])
		}
		assert.Equal(t, []string{"1"}, keys)
	})
}
