package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/stretchr/testify/assert"
)

func TestCreateIndexKeyColMeta(t *testing.T) {
	t.Run("インデックスキーカラムメタデータを新規作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupDictTestBufferPool(t)

		// WHEN
		kcm, err := CreateIndexKeyColMeta(bp)

		// THEN
		assert.NoError(t, err)
		assert.False(t, kcm.tree.MetaPageId.IsInvalid())
	})
}

func TestIndexKeyColMetaInsert(t *testing.T) {
	t.Run("インデックスキーカラムメタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		kcm := setupTestIndexKeyColMeta(t)

		// WHEN
		err := kcm.Insert(IndexId(1), "name", 1)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同じインデックス ID + カラム名の重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		kcm := setupTestIndexKeyColMeta(t)
		_ = kcm.Insert(IndexId(1), "name", 1)

		// WHEN
		err := kcm.Insert(IndexId(1), "name", 2)

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("同じインデックス ID でもカラム名が異なれば複数挿入できる", func(t *testing.T) {
		// GIVEN
		kcm := setupTestIndexKeyColMeta(t)
		_ = kcm.Insert(IndexId(1), "name", 1)

		// WHEN
		err := kcm.Insert(IndexId(1), "age", 2)

		// THEN
		assert.NoError(t, err)
	})
}

func TestIndexKeyColMetaSearch(t *testing.T) {
	t.Run("SearchModeStart で全件スキャンできる", func(t *testing.T) {
		// GIVEN
		kcm := setupTestIndexKeyColMeta(t)
		_ = kcm.Insert(IndexId(1), "name", 1)
		_ = kcm.Insert(IndexId(1), "age", 2)

		// WHEN
		iter, err := kcm.Search(SearchModeStart{})
		assert.NoError(t, err)

		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, IndexId(1), r1.IndexId)
		assert.Equal(t, "age", r1.Name)
		assert.Equal(t, 2, r1.Pos)

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, IndexId(1), r2.IndexId)
		assert.Equal(t, "name", r2.Name)
		assert.Equal(t, 1, r2.Pos)

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("空のメタデータを検索するとレコードが返らない", func(t *testing.T) {
		// GIVEN
		kcm := setupTestIndexKeyColMeta(t)

		// WHEN
		iter, err := kcm.Search(SearchModeStart{})
		assert.NoError(t, err)

		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

// setupTestIndexKeyColMeta はテスト用の IndexKeyColMeta を作成する
func setupTestIndexKeyColMeta(t *testing.T) *IndexKeyColMeta {
	t.Helper()
	bp := setupDictTestBufferPool(t)
	kcm, err := CreateIndexKeyColMeta(bp)
	if err != nil {
		t.Fatalf("IndexKeyColMeta の作成に失敗: %v", err)
	}
	return kcm
}
