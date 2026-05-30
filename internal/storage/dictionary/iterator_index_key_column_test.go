package dictionary

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/stretchr/testify/assert"
)

func TestIndexKeyColumnIteratorClose(t *testing.T) {
	t.Run("検索結果のイテレータを Close できる", func(t *testing.T) {
		// GIVEN
		kcm, bp := setupTestIndexKeyColumnMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		_ = kcm.Insert(mtr, NewIndexKeyColumnMetaRecord(IndexId(1), "name", 1))
		iter, err := kcm.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		// WHEN
		// THEN
		_, _, _ = iter.Next()
		iter.Close()

	})

	t.Run("イテレーション前に Close できる", func(t *testing.T) {
		// GIVEN
		kcm, bp := setupTestIndexKeyColumnMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		iter, err := kcm.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		// WHEN
		// THEN
		iter.Close()
	})
}

func TestIndexKeyColumnIteratorNext(t *testing.T) {
	t.Run("レコードを順に取得できる", func(t *testing.T) {
		// GIVEN
		kcm, bp := setupTestIndexKeyColumnMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		_ = kcm.Insert(mtr, NewIndexKeyColumnMetaRecord(IndexId(1), "name", 1))
		_ = kcm.Insert(mtr, NewIndexKeyColumnMetaRecord(IndexId(1), "age", 2))
		iter, err := kcm.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)
		defer iter.Close()

		// WHEN
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
		assert.Equal(t, "name", r2.Name())
		assert.Equal(t, 1, r2.Position())

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("空のメタデータの場合は false を返す", func(t *testing.T) {
		// GIVEN
		kcm, bp := setupTestIndexKeyColumnMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		iter, err := kcm.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)
		defer iter.Close()

		// WHEN
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}
