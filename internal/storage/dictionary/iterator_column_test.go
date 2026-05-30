package dictionary

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestColumnIteratorClose(t *testing.T) {
	t.Run("検索結果のイテレータを Close できる", func(t *testing.T) {
		// GIVEN
		cm, bp := setupTestColumnMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		_ = cm.Insert(mtr, NewColumnMetaRecord(page.FileId(1), "id", 0))
		iter, err := cm.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		// WHEN
		_, _, _ = iter.Next()
		iter.Close()

		// THEN: パニックせずに Close できる
	})

	t.Run("イテレーション前に Close できる", func(t *testing.T) {
		// GIVEN
		cm, bp := setupTestColumnMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		iter, err := cm.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		// WHEN
		// THEN
		iter.Close()
	})
}

func TestColumnIteratorNext(t *testing.T) {
	t.Run("レコードを順に取得できる", func(t *testing.T) {
		// GIVEN
		cm, bp := setupTestColumnMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		_ = cm.Insert(mtr, NewColumnMetaRecord(page.FileId(1), "id", 0))
		_ = cm.Insert(mtr, NewColumnMetaRecord(page.FileId(1), "name", 1))
		iter, err := cm.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)
		defer iter.Close()

		// WHEN
		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, page.FileId(1), r1.FileId())
		assert.Equal(t, "id", r1.Name())
		assert.Equal(t, 0, r1.Position())

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, "name", r2.Name())
		assert.Equal(t, 1, r2.Position())

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("空のメタデータの場合は false を返す", func(t *testing.T) {
		// GIVEN
		cm, bp := setupTestColumnMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		iter, err := cm.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)
		defer iter.Close()

		// WHEN
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}
