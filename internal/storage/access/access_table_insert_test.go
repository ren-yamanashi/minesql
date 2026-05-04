package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/stretchr/testify/assert"
)

func TestTableInsert(t *testing.T) {
	t.Run("テーブルにレコードを挿入できる", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, err := NewTable(env.bp, env.ct, "users")
		assert.NoError(t, err)

		// WHEN
		err = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)

		// THEN
		assert.NoError(t, err)
		record := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, record.Values)
	})

	t.Run("セカンダリインデックスにも挿入される", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t)

		// THEN
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.Search(SearchModeStart{})
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", nameResult.Values[1])
		idxEmail := findSecondaryIndex(t, table, "idx_email")
		emailIter, err := idxEmail.Search(SearchModeStart{})
		assert.NoError(t, err)
		emailResult, ok, err := emailIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "alice@example.com", emailResult.Values[2])
	})

	t.Run("異なるプライマリキーで複数レコードを挿入できる", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t)

		// WHEN
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "bob@example.com"},
		)

		// THEN
		assert.NoError(t, err)
		first := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Alice", first.Values[1])
	})

	t.Run("同一プライマリキーで挿入すると ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t)

		// WHEN
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Bob", "bob@example.com"},
		)

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("論理削除済みの同一プライマリキーに再挿入できる", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t) // id=1, Alice
		record := searchFirstPrimaryRecord(t, table)
		err := table.SoftDelete(record)
		assert.NoError(t, err)

		// WHEN
		err = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Charlie", "charlie@example.com"},
		)

		// THEN
		assert.NoError(t, err)
		reinserted := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Charlie", reinserted.Values[1])
	})

	t.Run("カラム数が不足しているとエラーを返す", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, err := NewTable(env.bp, env.ct, "users")
		assert.NoError(t, err)

		// WHEN
		err = table.Insert(
			[]string{"id", "name"},
			[]string{"1", "Alice"},
		)

		// THEN
		assert.Error(t, err)
	})
}
