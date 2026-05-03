package dictionary

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestTableMetaInsert(t *testing.T) {
	t.Run("テーブルメタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		tm := setupTestTableMeta(t)

		// WHEN
		err := tm.Insert(page.FileId(1), "users", 3)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同じ FileId の重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTestTableMeta(t)
		_ = tm.Insert(page.FileId(1), "users", 3)

		// WHEN
		err := tm.Insert(page.FileId(1), "orders", 5)

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("異なる FileId であれば複数挿入できる", func(t *testing.T) {
		// GIVEN
		tm := setupTestTableMeta(t)
		_ = tm.Insert(page.FileId(1), "users", 3)

		// WHEN
		err := tm.Insert(page.FileId(2), "orders", 5)

		// THEN
		assert.NoError(t, err)
	})
}

func TestTableMetaSearch(t *testing.T) {
	t.Run("SearchModeStart で全件スキャンできる", func(t *testing.T) {
		// GIVEN
		tm := setupTestTableMeta(t)
		_ = tm.Insert(page.FileId(1), "users", 3)
		_ = tm.Insert(page.FileId(2), "orders", 5)

		// WHEN
		iter, err := tm.Search(SearchModeStart{})
		assert.NoError(t, err)

		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, page.FileId(1), r1.fileId)
		assert.Equal(t, "users", r1.name)
		assert.Equal(t, 3, r1.numOfCol)

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, page.FileId(2), r2.fileId)
		assert.Equal(t, "orders", r2.name)
		assert.Equal(t, 5, r2.numOfCol)

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
	tree, err := btree.CreateBtree(bp, page.FileId(0))
	if err != nil {
		t.Fatalf("B+Tree の作成に失敗: %v", err)
	}
	return &TableMeta{tree: tree}
}
