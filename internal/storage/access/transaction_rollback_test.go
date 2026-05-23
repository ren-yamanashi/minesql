package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRollbackInsertSecondary(t *testing.T) {
	t.Run("Insert のロールバックでセカンダリインデックスからも物理削除される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trxId := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId,
		)
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trxId)

		// THEN
		assert.NoError(t, err)

		// idx_name からも削除されている
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.search(SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := nameIter.next()
		assert.NoError(t, err)
		assert.False(t, ok)

		// idx_email からも削除されている
		idxEmail := findSecondaryIndex(t, table, "idx_email")
		emailIter, err := idxEmail.search(SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err = emailIter.next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestRollbackDeleteSecondary(t *testing.T) {
	t.Run("SoftDelete のロールバックでセカンダリインデックスも復元される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trxId := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		_ = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId,
		)
		_ = tm.Commit(trxId)

		trxId2 := tm.Begin()
		iter, _ := table.primaryIndex.search(SearchModeStart{})
		record, _, _ := iter.next()
		err := table.SoftDelete(record, trxId2)
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trxId2)

		// THEN
		assert.NoError(t, err)

		// idx_name が復元されている
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.search(SearchModeStart{})
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", nameResult.values[1])
	})
}

func TestRollbackUpdateSecondary(t *testing.T) {
	t.Run("SK が変わる Update のロールバックでセカンダリインデックスが復元される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trxId := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		_ = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId,
		)
		_ = tm.Commit(trxId)

		trxId2 := tm.Begin()
		iter, _ := table.primaryIndex.search(SearchModeStart{})
		record, _, _ := iter.next()
		// name を変更 → idx_name の SK が変わる
		err := table.Update(record, []string{"name"}, []string{"Bob"}, trxId2)
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trxId2)

		// THEN
		assert.NoError(t, err)

		// idx_name が元の "Alice" に戻っている
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.search(SearchModeStart{})
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", nameResult.values[1])
	})

	t.Run("SK が変わらない Update のロールバックではセカンダリインデックスはそのまま", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trxId := tm.Begin()
		table := setupTableForTrxTest(t, tm)
		_ = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId,
		)
		_ = tm.Commit(trxId)

		trxId2 := tm.Begin()
		iter, _ := table.primaryIndex.search(SearchModeStart{})
		record, _, _ := iter.next()
		// email を変更 → idx_name の SK は変わらない
		err := table.Update(record, []string{"email"}, []string{"new@example.com"}, trxId2)
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trxId2)

		// THEN
		assert.NoError(t, err)

		// idx_name は変わらず "Alice"
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.search(SearchModeStart{})
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", nameResult.values[1])
	})
}
