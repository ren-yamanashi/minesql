package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/stretchr/testify/assert"
)

func TestCreateIndexKeyColumnMeta(t *testing.T) {
	t.Run("インデックスキーカラムメタデータを新規作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupDictTestBufferPool(t)

		// WHEN
		kcm, err := CreateIndexKeyColumnMeta(bp)

		// THEN
		assert.NoError(t, err)
		assert.False(t, kcm.tree.MetaPageId().IsInvalid())
	})
}

func TestIndexKeyColumnMetaInsert(t *testing.T) {
	t.Run("インデックスキーカラムメタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		kcm := setupTestIndexKeyColumnMeta(t)

		// WHEN
		err := kcm.Insert(NewIndexKeyColumnRecord(IndexId(1), "name", 1))

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同じインデックス ID + カラム名の重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		kcm := setupTestIndexKeyColumnMeta(t)
		_ = kcm.Insert(NewIndexKeyColumnRecord(IndexId(1), "name", 1))

		// WHEN
		err := kcm.Insert(NewIndexKeyColumnRecord(IndexId(1), "name", 2))

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("同じインデックス ID でもカラム名が異なれば複数挿入できる", func(t *testing.T) {
		// GIVEN
		kcm := setupTestIndexKeyColumnMeta(t)
		_ = kcm.Insert(NewIndexKeyColumnRecord(IndexId(1), "name", 1))

		// WHEN
		err := kcm.Insert(NewIndexKeyColumnRecord(IndexId(1), "age", 2))

		// THEN
		assert.NoError(t, err)
	})
}

func TestIndexKeyColumnMetaSearch(t *testing.T) {
	t.Run("SearchModeStart で全件スキャンできる", func(t *testing.T) {
		// GIVEN
		kcm := setupTestIndexKeyColumnMeta(t)
		_ = kcm.Insert(NewIndexKeyColumnRecord(IndexId(1), "name", 1))
		_ = kcm.Insert(NewIndexKeyColumnRecord(IndexId(1), "age", 2))

		// WHEN
		iter, err := kcm.Search(SearchModeStart{})
		assert.NoError(t, err)

		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, IndexId(1), r1.IndexId())
		assert.Equal(t, "age", r1.Name())
		assert.Equal(t, 2, r1.Position())

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, IndexId(1), r2.IndexId())
		assert.Equal(t, "name", r2.Name())
		assert.Equal(t, 1, r2.Position())

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("空のメタデータを検索するとレコードが返らない", func(t *testing.T) {
		// GIVEN
		kcm := setupTestIndexKeyColumnMeta(t)

		// WHEN
		iter, err := kcm.Search(SearchModeStart{})
		assert.NoError(t, err)

		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

// setupTestIndexKeyColumnMeta はテスト用の IndexKeyColumnMeta を作成する
func setupTestIndexKeyColumnMeta(t *testing.T) *IndexKeyColumnMeta {
	t.Helper()
	bp := setupDictTestBufferPool(t)
	kcm, err := CreateIndexKeyColumnMeta(bp)
	if err != nil {
		t.Fatalf("IndexKeyColumnMeta の作成に失敗: %v", err)
	}
	return kcm
}
