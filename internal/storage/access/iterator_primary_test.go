package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestPrimaryIteratorNext(t *testing.T) {
	t.Run("プライマリレコードを取得できる", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		insertPrimaryRecord(t, env, 0, []string{"id", "name", "email"}, []string{"1", "Alice", "alice@example.com"})

		iter := searchPrimaryIndex(t, env)

		// WHEN
		result, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []string{"id", "name", "email"}, result.ColNames)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, result.Values)
	})

	t.Run("複数レコードを順に取得できる", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		insertPrimaryRecord(t, env, 0, []string{"id", "name", "email"}, []string{"1", "Alice", "a@example.com"})
		insertPrimaryRecord(t, env, 0, []string{"id", "name", "email"}, []string{"2", "Bob", "b@example.com"})

		iter := searchPrimaryIndex(t, env)

		// WHEN
		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, "1", r1.Values[0])

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, "2", r2.Values[0])

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("論理削除されたレコードをスキップする", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		insertPrimaryRecord(t, env, 1, []string{"id", "name", "email"}, []string{"1", "Alice", "a@example.com"})
		insertPrimaryRecord(t, env, 0, []string{"id", "name", "email"}, []string{"2", "Bob", "b@example.com"})

		iter := searchPrimaryIndex(t, env)

		// WHEN
		result, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "2", result.Values[0])
	})

	t.Run("全レコードが論理削除済みの場合データなしを返す", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		insertPrimaryRecord(t, env, 1, []string{"id", "name", "email"}, []string{"1", "Alice", "a@example.com"})

		iter := searchPrimaryIndex(t, env)

		// WHEN
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("空のインデックスから取得するとデータなしを返す", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		iter := searchPrimaryIndex(t, env)

		// WHEN
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

// searchPrimaryIndex はプライマリ B+Tree を先頭から検索してイテレータを返す
func searchPrimaryIndex(t *testing.T, env *iteratorTestEnv) *PrimaryIterator {
	t.Helper()
	mode := SearchModeStart{}
	iter, err := env.primaryTree.Search(mode.encode())
	if err != nil {
		t.Fatalf("プライマリインデックスの検索に失敗: %v", err)
	}
	return newPrimaryIterator(iter, env.ct, page.FileId(2))
}
