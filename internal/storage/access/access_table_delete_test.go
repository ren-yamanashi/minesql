package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableSoftDelete(t *testing.T) {
	t.Run("プライマリインデックスからレコードが論理削除される", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t)
		record := searchFirstPrimaryRecord(t, table)

		// WHEN
		err := table.SoftDelete(record)

		// THEN
		assert.NoError(t, err)
		iter, err := table.primaryIndex.Search(SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("セカンダリインデックスからもレコードが論理削除される", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t)
		record := searchFirstPrimaryRecord(t, table)

		// WHEN
		err := table.SoftDelete(record)

		// THEN
		assert.NoError(t, err)
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.Search(SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
		idxEmail := findSecondaryIndex(t, table, "idx_email")
		emailIter, err := idxEmail.Search(SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err = emailIter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("論理削除後に同一プライマリキーで再挿入できる", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t)
		record := searchFirstPrimaryRecord(t, table)
		err := table.SoftDelete(record)
		assert.NoError(t, err)

		// WHEN
		err = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Bob", "bob@example.com"},
		)

		// THEN
		assert.NoError(t, err)
		reinserted := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Bob", reinserted.Values[1])
		assert.Equal(t, "bob@example.com", reinserted.Values[2])
	})

	t.Run("存在しないレコードを論理削除するとエラーを返す", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, err := NewTable(env.bp, env.ct, "users")
		assert.NoError(t, err)
		fakeRecord := &PrimaryRecord{
			pkCount:    1,
			deleteMark: 0,
			ColNames:   []string{"id", "name", "email"},
			Values:     []string{"999", "Nobody", "nobody@example.com"},
		}

		// WHEN
		err = table.SoftDelete(fakeRecord)

		// THEN
		assert.Error(t, err)
	})

	t.Run("複数レコードのうち 1 件だけ論理削除できる", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t)
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "bob@example.com"},
		)
		assert.NoError(t, err)
		alice := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Alice", alice.Values[1])

		// WHEN
		err = table.SoftDelete(alice)

		// THEN
		assert.NoError(t, err)
		remaining := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Bob", remaining.Values[1])
		assert.Equal(t, "bob@example.com", remaining.Values[2])
	})
}
