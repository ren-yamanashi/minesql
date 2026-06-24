package dictionary

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestConstraintIteratorNext(t *testing.T) {
	t.Run("レコードを順に取得できる", func(t *testing.T) {
		// GIVEN
		cm, bp := setupTestConstraintMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		_ = cm.Insert(mtr, NewConstraintMetaRecord(page.FileId(1), "id", "PRIMARY", page.FileId(0), ""))
		_ = cm.Insert(mtr, NewConstraintMetaRecord(page.FileId(2), "user_id", "fk_orders_users", page.FileId(1), "id"))
		iter, err := cm.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		// WHEN
		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, page.FileId(1), r1.FileId())
		assert.Equal(t, "id", r1.ColumnName())
		assert.Equal(t, "PRIMARY", r1.ConstraintName())
		assert.Equal(t, page.FileId(0), r1.ReferenceTableFileId())
		assert.Equal(t, "", r1.ReferenceColumnName())

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, page.FileId(2), r2.FileId())
		assert.Equal(t, "user_id", r2.ColumnName())
		assert.Equal(t, "fk_orders_users", r2.ConstraintName())
		assert.Equal(t, page.FileId(1), r2.ReferenceTableFileId())
		assert.Equal(t, "id", r2.ReferenceColumnName())

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("空のメタデータの場合は false を返す", func(t *testing.T) {
		// GIVEN
		cm, bp := setupTestConstraintMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		iter, err := cm.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		// WHEN
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}
