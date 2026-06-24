package dictionary

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestTableIteratorNext(t *testing.T) {
	t.Run("レコードを順に取得できる", func(t *testing.T) {
		// GIVEN
		tm, bp := setupTestTableMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		_ = tm.Insert(mtr, NewTableMetaRecord("users", page.NewId(page.FileId(1), page.PageNumber(0)), 3))
		_ = tm.Insert(mtr, NewTableMetaRecord("orders", page.NewId(page.FileId(2), page.PageNumber(0)), 5))
		iter, err := tm.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		// WHEN
		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, "orders", r1.Name())
		assert.Equal(t, page.NewId(page.FileId(2), page.PageNumber(0)), r1.MetaPageId())
		assert.Equal(t, 5, r1.ColumnCount())

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, "users", r2.Name())
		assert.Equal(t, page.NewId(page.FileId(1), page.PageNumber(0)), r2.MetaPageId())
		assert.Equal(t, 3, r2.ColumnCount())

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("空のメタデータの場合は false を返す", func(t *testing.T) {
		// GIVEN
		tm, bp := setupTestTableMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		iter, err := tm.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		// WHEN
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}
