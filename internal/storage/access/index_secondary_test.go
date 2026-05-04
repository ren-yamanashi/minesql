package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewSecondaryIndex(t *testing.T) {
	t.Run("既存のセカンダリインデックスを開ける", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		created, err := CreateSecondaryIndex(env.ct, env.bp, CreateSecondaryIndexInput{
			FileId:      page.FileId(2),
			PrimaryTree: env.primaryTree,
			IndexName:   "idx_name",
			IsUnique:    false,
		})
		assert.NoError(t, err)

		// WHEN
		si := NewSecondaryIndex(env.ct, env.bp, NewSecondaryIndexInput{
			MetaPageId:  created.tree.MetaPageId,
			PrimaryTree: env.primaryTree,
			IndexName:   "idx_name",
			IsUnique:    false,
		})

		// THEN
		assert.NotNil(t, si)
	})
}

func TestCreateSecondaryIndex(t *testing.T) {
	t.Run("セカンダリインデックスを新規作成できる", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)

		// WHEN
		si, err := CreateSecondaryIndex(env.ct, env.bp, CreateSecondaryIndexInput{
			FileId:      page.FileId(2),
			PrimaryTree: env.primaryTree,
			IndexName:   "idx_name",
			IsUnique:    false,
		})

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, si)
	})
}

func TestSecondaryIndexInsert(t *testing.T) {
	t.Run("セカンダリインデックスにレコードを挿入できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)

		// WHEN
		err := si.Insert([]string{"name"}, []string{"Alice"}, []string{"1"})

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同一キー (SK+PK) の重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		_ = si.Insert([]string{"name"}, []string{"Alice"}, []string{"1"})

		// WHEN
		err := si.Insert([]string{"name"}, []string{"Alice"}, []string{"1"})

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("非ユニークインデックスでは異なる PK で同じ SK を挿入できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		_ = si.Insert([]string{"name"}, []string{"Alice"}, []string{"1"})

		// WHEN
		err := si.Insert([]string{"name"}, []string{"Alice"}, []string{"2"})

		// THEN
		assert.NoError(t, err)
	})

	t.Run("ユニークインデックスでは同じ SK の挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_email", true)
		_ = si.Insert([]string{"email"}, []string{"alice@example.com"}, []string{"1"})

		// WHEN
		err := si.Insert([]string{"email"}, []string{"alice@example.com"}, []string{"2"})

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("ユニークインデックスで論理削除済みの SK と同じ値は挿入できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_email", true)
		err := si.Insert([]string{"email"}, []string{"alice@example.com"}, []string{"1"})
		assert.NoError(t, err)

		// 論理削除
		iter, err := si.Search(SearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.NextIndexOnly()
		assert.NoError(t, err)
		assert.True(t, ok)
		err = si.SoftDelete(record)
		assert.NoError(t, err)

		// WHEN
		err = si.Insert([]string{"email"}, []string{"alice@example.com"}, []string{"2"})

		// THEN
		assert.NoError(t, err)
	})

	t.Run("論理削除済みの同一キー (SK+PK) がある場合は上書きできる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		err := si.Insert([]string{"name"}, []string{"Alice"}, []string{"1"})
		assert.NoError(t, err)

		// 論理削除
		iter, err := si.Search(SearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.NextIndexOnly()
		assert.NoError(t, err)
		assert.True(t, ok)
		err = si.SoftDelete(record)
		assert.NoError(t, err)

		// WHEN
		err = si.Insert([]string{"name"}, []string{"Alice"}, []string{"1"})

		// THEN
		assert.NoError(t, err)
	})
}

func TestSecondaryIndexSearch(t *testing.T) {
	t.Run("全件スキャンでレコードを取得できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		_ = si.Insert([]string{"name"}, []string{"Alice"}, []string{"1"})

		// WHEN
		iter, err := si.Search(SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		result, ok, err := iter.NextIndexOnly()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []string{"Alice"}, result.Values)
	})
}

func TestSecondaryIndexDelete(t *testing.T) {
	t.Run("レコードを物理削除できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		_ = si.Insert([]string{"name"}, []string{"Alice"}, []string{"1"})

		iter, _ := si.Search(SearchModeStart{})
		record, _, _ := iter.NextIndexOnly()

		// WHEN
		err := si.Delete(record)

		// THEN
		assert.NoError(t, err)

		// 削除後は取得できない
		iter2, _ := si.Search(SearchModeStart{})
		_, ok, _ := iter2.NextIndexOnly()
		assert.False(t, ok)
	})

	t.Run("存在しないレコードの削除はエラーを返す", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		sr := &SecondaryRecord{
			ColNames: []string{"name"},
			Values:   []string{"nonexistent"},
			Pk:       []string{"999"},
		}

		// WHEN
		err := si.Delete(sr)

		// THEN
		assert.Error(t, err)
	})
}

func TestSecondaryIndexSoftDelete(t *testing.T) {
	t.Run("レコードを論理削除できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		_ = si.Insert([]string{"name"}, []string{"Alice"}, []string{"1"})

		iter, _ := si.Search(SearchModeStart{})
		record, _, _ := iter.NextIndexOnly()

		// WHEN
		err := si.SoftDelete(record)

		// THEN
		assert.NoError(t, err)

		// 論理削除後は検索でスキップされる
		iter2, _ := si.Search(SearchModeStart{})
		_, ok, _ := iter2.NextIndexOnly()
		assert.False(t, ok)
	})

	t.Run("論理削除後に再挿入できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		_ = si.Insert([]string{"name"}, []string{"Alice"}, []string{"1"})

		iter, _ := si.Search(SearchModeStart{})
		record, _, _ := iter.NextIndexOnly()
		_ = si.SoftDelete(record)

		// WHEN
		err := si.Insert([]string{"name"}, []string{"Alice"}, []string{"1"})

		// THEN
		assert.NoError(t, err)
	})
}

func TestSecondaryIndexLeafPageCount(t *testing.T) {
	t.Run("リーフページ数を取得できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)

		// WHEN
		count, err := si.LeafPageCount()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})
}

func TestSecondaryIndexHeight(t *testing.T) {
	t.Run("ツリーの高さを取得できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)

		// WHEN
		height, err := si.Height()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})
}

// setupTestSecondaryIndex はテスト用の SecondaryIndex を作成する
func setupTestSecondaryIndex(t *testing.T, indexName string, isUnique bool) *SecondaryIndex {
	t.Helper()
	env := setupIteratorTestEnv(t)
	si, err := CreateSecondaryIndex(env.ct, env.bp, CreateSecondaryIndexInput{
		FileId:      page.FileId(2),
		PrimaryTree: env.primaryTree,
		IndexName:   indexName,
		IsUnique:    isUnique,
	})
	if err != nil {
		t.Fatalf("SecondaryIndex の作成に失敗: %v", err)
	}
	return si
}
