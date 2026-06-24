package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/stretchr/testify/assert"
)

func TestTableSearch(t *testing.T) {
	t.Run("最初の呼び出しで ReadView が自動作成される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		table := setupTableForTrxTest(t, tm)
		trx := tm.Begin()
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		_, err := table.Search(mtr, trx, SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, trx.readView)
	})

	t.Run("同一 trxId で複数回呼ぶと同じ ReadView を使い回す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		table := setupTableForTrxTest(t, tm)
		trx := tm.Begin()
		mtr1 := buffer.NewMtr(table.bufferPool)
		defer mtr1.UnpinAll()
		mtr2 := buffer.NewMtr(table.bufferPool)
		defer mtr2.UnpinAll()

		// WHEN
		_, err1 := table.Search(mtr1, trx, SearchModeStart{})
		rv1 := trx.readView
		_, err2 := table.Search(mtr2, trx, SearchModeStart{})
		rv2 := trx.readView

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.Same(t, rv1, rv2)
	})

	t.Run("挿入済みのレコードを Search で取得できる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		table := setupTableForTrxTest(t, tm)
		writeTx := tm.Begin()
		_ = table.Insert(
			writeTx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		_ = tm.Commit(writeTx)

		readTx := tm.Begin()
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		iter, err := table.Search(mtr, readTx, SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		result, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", result.values[1])
	})
}

func TestTableSearchSecondary(t *testing.T) {
	t.Run("存在しないインデックス名でエラーを返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		table := setupTableForTrxTest(t, tm)
		trx := tm.Begin()
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		_, err := table.SearchSecondary(mtr, trx, "nonexistent", SearchModeStart{})

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nonexistent")
	})

	t.Run("セカンダリインデックス経由でレコードを取得できる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		table := setupTableForTrxTest(t, tm)
		writeTx := tm.Begin()
		_ = table.Insert(
			writeTx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		_ = tm.Commit(writeTx)

		readTx := tm.Begin()
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		iter, err := table.SearchSecondary(mtr, readTx, "idx_name", SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		result, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", result.values[1])
	})
}
