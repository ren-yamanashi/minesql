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
		trxId := tm.Begin()
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		_, err := table.Search(tm, trxId, mtr, SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		_, ok := tm.readViews[trxId]
		assert.True(t, ok)
	})

	t.Run("同一 trxId で複数回呼ぶと同じ ReadView を使い回す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		table := setupTableForTrxTest(t, tm)
		trxId := tm.Begin()
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		_, err1 := table.Search(tm, trxId, mtr, SearchModeStart{})
		rv1 := tm.readViews[trxId]
		_, err2 := table.Search(tm, trxId, mtr, SearchModeStart{})
		rv2 := tm.readViews[trxId]

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.Same(t, rv1, rv2)
	})

	t.Run("挿入済みのレコードを Search で取得できる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		table := setupTableForTrxTest(t, tm)
		writeTrx := tm.Begin()
		_ = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			writeTrx,
		)
		_ = tm.Commit(writeTrx)

		readTrx := tm.Begin()
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		iter, err := table.Search(tm, readTrx, mtr, SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		defer iter.Close()
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
		trxId := tm.Begin()
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		_, err := table.SearchSecondary(tm, trxId, mtr, "nonexistent", SearchModeStart{})

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nonexistent")
	})

	t.Run("セカンダリインデックス経由でレコードを取得できる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		table := setupTableForTrxTest(t, tm)
		writeTrx := tm.Begin()
		_ = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			writeTrx,
		)
		_ = tm.Commit(writeTrx)

		readTrx := tm.Begin()
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		iter, err := table.SearchSecondary(tm, readTrx, mtr, "idx_name", SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		defer iter.Close()
		result, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", result.values[1])
	})
}
