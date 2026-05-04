package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestCreateIndexMeta(t *testing.T) {
	t.Run("インデックスメタデータを新規作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupDictTestBufferPool(t)

		// WHEN
		im, err := CreateIndexMeta(bp)

		// THEN
		assert.NoError(t, err)
		assert.False(t, im.metaPageId.IsInvalid())
	})
}

func TestIndexMetaInsert(t *testing.T) {
	t.Run("インデックスメタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		im := setupTestIndexMeta(t)

		// WHEN
		err := im.Insert(page.FileId(1), "PRIMARY", IndexId(1), IndexTypePrimary, 1)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同じ FileId + インデックス名の重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		im := setupTestIndexMeta(t)
		_ = im.Insert(page.FileId(1), "PRIMARY", IndexId(1), IndexTypePrimary, 1)

		// WHEN
		err := im.Insert(page.FileId(1), "PRIMARY", IndexId(2), IndexTypePrimary, 1)

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("異なるインデックス名であれば同じテーブルに複数挿入できる", func(t *testing.T) {
		// GIVEN
		im := setupTestIndexMeta(t)
		_ = im.Insert(page.FileId(1), "PRIMARY", IndexId(1), IndexTypePrimary, 1)

		// WHEN
		err := im.Insert(page.FileId(1), "idx_email", IndexId(2), IndexTypeUnique, 1)

		// THEN
		assert.NoError(t, err)
	})
}

func TestIndexMetaSearch(t *testing.T) {
	t.Run("SearchModeStart で全件スキャンできる", func(t *testing.T) {
		// GIVEN
		im := setupTestIndexMeta(t)
		_ = im.Insert(page.FileId(1), "PRIMARY", IndexId(1), IndexTypePrimary, 1)
		_ = im.Insert(page.FileId(1), "idx_name", IndexId(2), IndexTypeNonUnique, 2)

		// THEN: インデックス名でソートされる
		iter, err := im.Search(SearchModeStart{})
		assert.NoError(t, err)

		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, page.FileId(1), r1.FileId)
		assert.Equal(t, "PRIMARY", r1.Name)
		assert.Equal(t, IndexId(1), r1.IndexId)
		assert.Equal(t, IndexTypePrimary, r1.IndexType)
		assert.Equal(t, 1, r1.NumOfCol)

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, "idx_name", r2.Name)
		assert.Equal(t, IndexId(2), r2.IndexId)
		assert.Equal(t, IndexTypeNonUnique, r2.IndexType)
		assert.Equal(t, 2, r2.NumOfCol)

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("空のメタデータを検索するとレコードが返らない", func(t *testing.T) {
		// GIVEN
		im := setupTestIndexMeta(t)

		// WHEN
		iter, err := im.Search(SearchModeStart{})
		assert.NoError(t, err)

		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

// setupTestIndexMeta はテスト用の IndexMeta を作成する
func setupTestIndexMeta(t *testing.T) *IndexMeta {
	t.Helper()
	bp := setupDictTestBufferPool(t)
	im, err := CreateIndexMeta(bp)
	if err != nil {
		t.Fatalf("IndexMeta の作成に失敗: %v", err)
	}
	return im
}
