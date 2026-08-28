package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableSearch(t *testing.T) {
	t.Run("最初の呼び出しで ReadView が自動作成される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		table := setupTableForTrxTest(t, tm)
		trx := tm.Begin()

		// WHEN
		_, err := table.Search(trx, SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, trx.readView)
	})

	t.Run("同一 trx で複数のイテレータを開いても同じ ReadView を使い回す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		table := setupTableForTrxTest(t, tm)
		trx := tm.Begin()

		// WHEN
		iter1, err1 := table.Search(trx, SearchModeStart{})
		rv1 := trx.readView
		iter2, err2 := table.Search(trx, SearchModeStart{})
		rv2 := trx.readView

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.Same(t, rv1, rv2)
		iter1.Close()
		iter2.Close()
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

		// WHEN
		iter, err := table.Search(readTx, SearchModeStart{})

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

		// WHEN
		_, err := table.SearchSecondary(trx, "nonexistent", SearchModeStart{})

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

		// WHEN
		iter, err := table.SearchSecondary(readTx, "idx_name", SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		result, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", result.values[1])
	})
}
