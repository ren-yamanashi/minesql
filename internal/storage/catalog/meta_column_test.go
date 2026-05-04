package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestCreateColumnMeta(t *testing.T) {
	t.Run("カラムメタデータを新規作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupDictTestBufferPool(t)

		// WHEN
		cm, err := CreateColumnMeta(bp)

		// THEN
		assert.NoError(t, err)
		assert.False(t, cm.tree.MetaPageId.IsInvalid())
	})
}

func TestColumnMetaInsert(t *testing.T) {
	t.Run("カラムメタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		cm := setupTestColumnMeta(t)

		// WHEN
		err := cm.Insert(page.FileId(1), "name", 0)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同じ FileId + カラム名の重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		cm := setupTestColumnMeta(t)
		_ = cm.Insert(page.FileId(1), "name", 0)

		// WHEN
		err := cm.Insert(page.FileId(1), "name", 1)

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("同じテーブルに異なるカラム名であれば複数挿入できる", func(t *testing.T) {
		// GIVEN
		cm := setupTestColumnMeta(t)
		_ = cm.Insert(page.FileId(1), "id", 0)

		// WHEN
		err := cm.Insert(page.FileId(1), "name", 1)

		// THEN
		assert.NoError(t, err)
	})
}

func TestColumnMetaSearch(t *testing.T) {
	t.Run("SearchModeStart で全件スキャンできる", func(t *testing.T) {
		// GIVEN
		cm := setupTestColumnMeta(t)
		_ = cm.Insert(page.FileId(1), "id", 0)
		_ = cm.Insert(page.FileId(1), "name", 1)

		// WHEN
		iter, err := cm.Search(SearchModeStart{})
		assert.NoError(t, err)

		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, page.FileId(1), r1.FileId)
		assert.Equal(t, "id", r1.Name)
		assert.Equal(t, 0, r1.Pos)

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, page.FileId(1), r2.FileId)
		assert.Equal(t, "name", r2.Name)
		assert.Equal(t, 1, r2.Pos)

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("空のメタデータを検索するとレコードが返らない", func(t *testing.T) {
		// GIVEN
		cm := setupTestColumnMeta(t)

		// WHEN
		iter, err := cm.Search(SearchModeStart{})
		assert.NoError(t, err)

		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

// setupTestColumnMeta はテスト用の ColumnMeta を作成する
func setupTestColumnMeta(t *testing.T) *ColumnMeta {
	t.Helper()
	bp := setupDictTestBufferPool(t)
	cm, err := CreateColumnMeta(bp)
	if err != nil {
		t.Fatalf("ColumnMeta の作成に失敗: %v", err)
	}
	return cm
}
