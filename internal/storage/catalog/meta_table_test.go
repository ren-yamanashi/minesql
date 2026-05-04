package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestCreateTableMeta(t *testing.T) {
	t.Run("テーブルメタデータを新規作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupDictTestBufferPool(t)

		// WHEN
		tm, err := CreateTableMeta(bp)

		// THEN
		assert.NoError(t, err)
		assert.False(t, tm.metaPageId.IsInvalid())
	})
}

func TestTableMetaInsert(t *testing.T) {
	t.Run("テーブルメタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		tm := setupTestTableMeta(t)

		// WHEN
		err := tm.Insert("users", page.FileId(1), 3)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同じテーブル名の重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTestTableMeta(t)
		_ = tm.Insert("users", page.FileId(1), 3)

		// WHEN
		err := tm.Insert("users", page.FileId(2), 5)

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("異なるテーブル名であれば複数挿入できる", func(t *testing.T) {
		// GIVEN
		tm := setupTestTableMeta(t)
		_ = tm.Insert("users", page.FileId(1), 3)

		// WHEN
		err := tm.Insert("orders", page.FileId(2), 5)

		// THEN
		assert.NoError(t, err)
	})
}

func TestTableMetaSearch(t *testing.T) {
	t.Run("SearchModeStart で全件スキャンできる", func(t *testing.T) {
		// GIVEN
		tm := setupTestTableMeta(t)
		_ = tm.Insert("users", page.FileId(1), 3)
		_ = tm.Insert("orders", page.FileId(2), 5)

		// WHEN
		iter, err := tm.Search(SearchModeStart{})
		assert.NoError(t, err)

		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN: テーブル名でソートされる
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, "orders", r1.Name)
		assert.Equal(t, page.FileId(2), r1.FileId)
		assert.Equal(t, 5, r1.NumOfCol)

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, "users", r2.Name)
		assert.Equal(t, page.FileId(1), r2.FileId)
		assert.Equal(t, 3, r2.NumOfCol)

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("空のメタデータを検索するとレコードが返らない", func(t *testing.T) {
		// GIVEN
		tm := setupTestTableMeta(t)

		// WHEN
		iter, err := tm.Search(SearchModeStart{})
		assert.NoError(t, err)

		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

// setupTestTableMeta はテスト用の TableMeta を作成する
func setupTestTableMeta(t *testing.T) *TableMeta {
	t.Helper()
	bp := setupDictTestBufferPool(t)
	tm, err := CreateTableMeta(bp)
	if err != nil {
		t.Fatalf("TableMeta の作成に失敗: %v", err)
	}
	return tm
}
