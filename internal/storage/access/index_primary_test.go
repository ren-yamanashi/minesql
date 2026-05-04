package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewPrimaryIndex(t *testing.T) {
	t.Run("既存のプライマリインデックスを開ける", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		created, err := CreatePrimaryIndex(env.ct, env.bp, page.FileId(2), 1)
		assert.NoError(t, err)

		// WHEN
		pi := NewPrimaryIndex(env.ct, env.bp, created.tree.MetaPageId, 1)

		// THEN
		assert.NotNil(t, pi)
	})
}

func TestCreatePrimaryIndex(t *testing.T) {
	t.Run("プライマリインデックスを新規作成できる", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)

		// WHEN
		pi, err := CreatePrimaryIndex(env.ct, env.bp, page.FileId(2), 1)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, pi)
	})
}

func TestPrimaryIndexInsert(t *testing.T) {
	t.Run("レコードを挿入できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)

		// WHEN
		err := pi.Insert([]string{"id", "name", "email"}, []string{"1", "Alice", "alice@example.com"})

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同一プライマリキーの重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		_ = pi.Insert([]string{"id", "name", "email"}, []string{"1", "Alice", "a@example.com"})

		// WHEN
		err := pi.Insert([]string{"id", "name", "email"}, []string{"1", "Bob", "b@example.com"})

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("異なるプライマリキーであれば複数挿入できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		_ = pi.Insert([]string{"id", "name", "email"}, []string{"1", "Alice", "a@example.com"})

		// WHEN
		err := pi.Insert([]string{"id", "name", "email"}, []string{"2", "Bob", "b@example.com"})

		// THEN
		assert.NoError(t, err)
	})

	t.Run("論理削除済みの同一キーがある場合は上書きできる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		_ = pi.Insert([]string{"id", "name", "email"}, []string{"1", "Alice", "a@example.com"})

		// 論理削除
		iter, _ := pi.Search(SearchModeStart{})
		record, _, _ := iter.Next()
		_ = pi.SoftDelete(record)

		// WHEN
		err := pi.Insert([]string{"id", "name", "email"}, []string{"1", "Bob", "b@example.com"})

		// THEN
		assert.NoError(t, err)
	})
}

func TestPrimaryIndexSearch(t *testing.T) {
	t.Run("全件スキャンでレコードを取得できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		_ = pi.Insert([]string{"id", "name", "email"}, []string{"1", "Alice", "alice@example.com"})

		// WHEN
		iter, err := pi.Search(SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		result, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, result.Values)
	})

	t.Run("空のインデックスを検索するとデータなしを返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)

		// WHEN
		iter, err := pi.Search(SearchModeStart{})
		assert.NoError(t, err)

		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestPrimaryIndexSoftDelete(t *testing.T) {
	t.Run("レコードを論理削除できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		_ = pi.Insert([]string{"id", "name", "email"}, []string{"1", "Alice", "alice@example.com"})

		iter, _ := pi.Search(SearchModeStart{})
		record, _, _ := iter.Next()

		// WHEN
		err := pi.SoftDelete(record)

		// THEN
		assert.NoError(t, err)

		// 論理削除後は検索でスキップされる
		iter2, _ := pi.Search(SearchModeStart{})
		_, ok, _ := iter2.Next()
		assert.False(t, ok)
	})

	t.Run("論理削除後に再挿入できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		_ = pi.Insert([]string{"id", "name", "email"}, []string{"1", "Alice", "alice@example.com"})

		iter, _ := pi.Search(SearchModeStart{})
		record, _, _ := iter.Next()
		_ = pi.SoftDelete(record)

		// WHEN
		err := pi.Insert([]string{"id", "name", "email"}, []string{"1", "Bob", "bob@example.com"})

		// THEN
		assert.NoError(t, err)
	})
}

func TestPrimaryIndexUpdateInplace(t *testing.T) {
	t.Run("レコードをインプレース更新できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		_ = pi.Insert([]string{"id", "name", "email"}, []string{"1", "Alice", "alice@example.com"})

		iter, _ := pi.Search(SearchModeStart{})
		current, _, _ := iter.Next()

		// WHEN
		err := pi.UpdateInplace(current, []string{"name"}, []string{"Bob"})

		// THEN
		assert.NoError(t, err)

		// 更新後の値を確認
		iter2, _ := pi.Search(SearchModeStart{})
		updated, ok, _ := iter2.Next()
		assert.True(t, ok)
		assert.Equal(t, "Bob", updated.Values[1])
		assert.Equal(t, "alice@example.com", updated.Values[2])
	})

	t.Run("存在しないカラムで更新するとエラーを返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		_ = pi.Insert([]string{"id", "name", "email"}, []string{"1", "Alice", "a@example.com"})

		iter, _ := pi.Search(SearchModeStart{})
		current, _, _ := iter.Next()

		// WHEN
		err := pi.UpdateInplace(current, []string{"nonexistent"}, []string{"val"})

		// THEN
		assert.Error(t, err)
	})
}

func TestPrimaryIndexDelete(t *testing.T) {
	t.Run("レコードを物理削除できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		_ = pi.Insert([]string{"id", "name", "email"}, []string{"1", "Alice", "alice@example.com"})

		iter, _ := pi.Search(SearchModeStart{})
		record, _, _ := iter.Next()

		// WHEN
		err := pi.delete(record)

		// THEN
		assert.NoError(t, err)

		// 削除後は取得できない
		iter2, _ := pi.Search(SearchModeStart{})
		_, ok, _ := iter2.Next()
		assert.False(t, ok)
	})

	t.Run("存在しないレコードの削除はエラーを返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		pr, _ := newPrimaryRecord(pi.catalog, newPrimaryRecordInput{fileId: pi.tree.MetaPageId.FileId, pkCount: 1, deleteMark: 0,
			colNames: []string{"id", "name", "email"}, values: []string{"999", "Nobody", "no@example.com"}})

		// WHEN
		err := pi.delete(pr)

		// THEN
		assert.Error(t, err)
	})
}

func TestPrimaryIndexLeafPageCount(t *testing.T) {
	t.Run("リーフページ数を取得できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)

		// WHEN
		count, err := pi.LeafPageCount()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})
}

func TestPrimaryIndexHeight(t *testing.T) {
	t.Run("ツリーの高さを取得できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)

		// WHEN
		height, err := pi.Height()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})
}

// setupTestPrimaryIndex はテスト用の PrimaryIndex (pkCount=1) を作成する
func setupTestPrimaryIndex(t *testing.T) *PrimaryIndex { //nolint:unparam
	t.Helper()
	env := setupIteratorTestEnv(t)
	pi, err := CreatePrimaryIndex(env.ct, env.bp, page.FileId(2), 1)
	if err != nil {
		t.Fatalf("PrimaryIndex の作成に失敗: %v", err)
	}
	return pi
}
