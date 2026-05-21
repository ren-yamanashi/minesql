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
		nameIter, err := idxName.Search(SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)

		// idx_email からも削除されている
		idxEmail := findSecondaryIndex(t, table, "idx_email")
		emailIter, err := idxEmail.Search(SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err = emailIter.Next()
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
		iter, _ := table.primaryIndex.Search(SearchModeStart{})
		record, _, _ := iter.Next()
		err := table.SoftDelete(record, trxId2)
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trxId2)

		// THEN
		assert.NoError(t, err)

		// idx_name が復元されている
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.Search(SearchModeStart{})
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", nameResult.Values[1])
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
		iter, _ := table.primaryIndex.Search(SearchModeStart{})
		record, _, _ := iter.Next()
		// name を変更 → idx_name の SK が変わる
		err := table.Update(record, []string{"name"}, []string{"Bob"}, trxId2)
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trxId2)

		// THEN
		assert.NoError(t, err)

		// idx_name が元の "Alice" に戻っている
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.Search(SearchModeStart{})
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", nameResult.Values[1])
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
		iter, _ := table.primaryIndex.Search(SearchModeStart{})
		record, _, _ := iter.Next()
		// email を変更 → idx_name の SK は変わらない
		err := table.Update(record, []string{"email"}, []string{"new@example.com"}, trxId2)
		assert.NoError(t, err)

		// WHEN
		err = tm.Rollback(trxId2)

		// THEN
		assert.NoError(t, err)

		// idx_name は変わらず "Alice"
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.Search(SearchModeStart{})
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", nameResult.Values[1])
	})
}

func TestEncodeSecondaryKey(t *testing.T) {
	t.Run("PrimaryRecord から SK+PK キーを構築できる", func(t *testing.T) {
		// GIVEN
		record := &PrimaryRecord{
			pkCount:  1,
			ColNames: []string{"id", "name", "email"},
			Values:   []string{"1", "Alice", "alice@example.com"},
		}
		keyCols := map[string]int{"name": 0}

		// WHEN
		key := encodeSecondaryKey(record, keyCols)

		// THEN
		assert.NotEmpty(t, key)
	})

	t.Run("同じ入力に対して同じキーを返す", func(t *testing.T) {
		// GIVEN
		record := &PrimaryRecord{
			pkCount:  1,
			ColNames: []string{"id", "name"},
			Values:   []string{"1", "Alice"},
		}
		keyCols := map[string]int{"name": 0}

		// WHEN
		key1 := encodeSecondaryKey(record, keyCols)
		key2 := encodeSecondaryKey(record, keyCols)

		// THEN
		assert.Equal(t, key1, key2)
	})

	t.Run("異なる SK 値に対して異なるキーを返す", func(t *testing.T) {
		// GIVEN
		record1 := &PrimaryRecord{
			pkCount:  1,
			ColNames: []string{"id", "name"},
			Values:   []string{"1", "Alice"},
		}
		record2 := &PrimaryRecord{
			pkCount:  1,
			ColNames: []string{"id", "name"},
			Values:   []string{"1", "Bob"},
		}
		keyCols := map[string]int{"name": 0}

		// WHEN
		key1 := encodeSecondaryKey(record1, keyCols)
		key2 := encodeSecondaryKey(record2, keyCols)

		// THEN
		assert.NotEqual(t, key1, key2)
	})

	t.Run("複合セカンダリキーを正しくエンコードする", func(t *testing.T) {
		// GIVEN
		record := &PrimaryRecord{
			pkCount:  1,
			ColNames: []string{"id", "name", "email"},
			Values:   []string{"1", "Alice", "alice@example.com"},
		}
		keyCols := map[string]int{"email": 0, "name": 1}

		// WHEN
		key := encodeSecondaryKey(record, keyCols)

		// THEN
		assert.NotEmpty(t, key)
	})
}
