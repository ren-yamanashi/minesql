package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestCreateConstraintMeta(t *testing.T) {
	t.Run("制約メタデータを新規作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupDictTestBufferPool(t)

		// WHEN
		cm, err := CreateConstraintMeta(bp)

		// THEN
		assert.NoError(t, err)
		assert.False(t, cm.tree.MetaPageId.IsInvalid())
	})
}

func TestConstraintMetaInsert(t *testing.T) {
	t.Run("主キー制約を挿入できる", func(t *testing.T) {
		// GIVEN
		cm := setupTestConstraintMeta(t)

		// WHEN
		err := cm.Insert(page.FileId(1), "id", "PRIMARY", page.FileId(0), "")

		// THEN
		assert.NoError(t, err)
	})

	t.Run("外部キー制約を挿入できる", func(t *testing.T) {
		// GIVEN
		cm := setupTestConstraintMeta(t)

		// WHEN
		err := cm.Insert(page.FileId(2), "user_id", "fk_orders_users", page.FileId(1), "id")

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同じ FileId + カラム名 + 制約名の重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		cm := setupTestConstraintMeta(t)
		_ = cm.Insert(page.FileId(1), "id", "PRIMARY", page.FileId(0), "")

		// WHEN
		err := cm.Insert(page.FileId(1), "id", "PRIMARY", page.FileId(0), "")

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("同じカラムに異なる制約名であれば複数挿入できる", func(t *testing.T) {
		// GIVEN
		cm := setupTestConstraintMeta(t)
		_ = cm.Insert(page.FileId(1), "email", "PRIMARY", page.FileId(0), "")

		// WHEN
		err := cm.Insert(page.FileId(1), "email", "idx_email", page.FileId(0), "")

		// THEN
		assert.NoError(t, err)
	})
}

func TestConstraintMetaSearch(t *testing.T) {
	t.Run("SearchModeStart で全件スキャンできる", func(t *testing.T) {
		// GIVEN
		cm := setupTestConstraintMeta(t)
		_ = cm.Insert(page.FileId(1), "id", "PRIMARY", page.FileId(0), "")
		_ = cm.Insert(page.FileId(2), "user_id", "fk_orders_users", page.FileId(1), "id")

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
		assert.Equal(t, "id", r1.ColName)
		assert.Equal(t, "PRIMARY", r1.ConstraintName)
		assert.Equal(t, page.FileId(0), r1.RefTableFileId)
		assert.Equal(t, "", r1.RefColName)

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, page.FileId(2), r2.FileId)
		assert.Equal(t, "user_id", r2.ColName)
		assert.Equal(t, "fk_orders_users", r2.ConstraintName)
		assert.Equal(t, page.FileId(1), r2.RefTableFileId)
		assert.Equal(t, "id", r2.RefColName)

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("空のメタデータを検索するとレコードが返らない", func(t *testing.T) {
		// GIVEN
		cm := setupTestConstraintMeta(t)

		// WHEN
		iter, err := cm.Search(SearchModeStart{})
		assert.NoError(t, err)

		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

// setupTestConstraintMeta はテスト用の ConstraintMeta を作成する
func setupTestConstraintMeta(t *testing.T) *ConstraintMeta {
	t.Helper()
	bp := setupDictTestBufferPool(t)
	cm, err := CreateConstraintMeta(bp)
	if err != nil {
		t.Fatalf("ConstraintMeta の作成に失敗: %v", err)
	}
	return cm
}
