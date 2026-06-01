package dictionary

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestIndexIteratorClose(t *testing.T) {
	t.Run("検索結果のイテレータを Close できる", func(t *testing.T) {
		// GIVEN
		im, bp := setupTestIndexMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		_ = im.Insert(mtr, NewIndexMetaRecord(
			page.FileId(1),
			IndexId(1),
			PrimaryIndexName,
			IndexTypePrimary,
			1,
			page.NewId(page.FileId(1), page.PageNumber(0)),
		))
		iter, err := im.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		// WHEN
		// THEN
		_, _, _ = iter.Next()
		iter.Close()

	})

	t.Run("イテレーション前に Close できる", func(t *testing.T) {
		// GIVEN
		im, bp := setupTestIndexMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		iter, err := im.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		// WHEN
		// THEN
		iter.Close()
	})
}

func TestIndexIteratorNext(t *testing.T) {
	t.Run("レコードを順に取得できる", func(t *testing.T) {
		// GIVEN
		im, bp := setupTestIndexMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		_ = im.Insert(mtr, NewIndexMetaRecord(
			page.FileId(1),
			IndexId(1),
			"PRIMARY",
			IndexTypePrimary,
			1,
			page.NewId(page.FileId(1), page.PageNumber(0)),
		))
		_ = im.Insert(mtr, NewIndexMetaRecord(
			page.FileId(1),
			IndexId(2),
			"idx_name",
			IndexTypeNonUnique,
			2,
			page.NewId(page.FileId(1), page.PageNumber(0)),
		))
		iter, err := im.Search(mtr, SearchModeStart{})
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
		assert.Equal(t, "PRIMARY", r1.Name())
		assert.Equal(t, IndexId(1), r1.IndexId())
		assert.Equal(t, IndexTypePrimary, r1.IndexType())
		assert.Equal(t, 1, r1.ColumnCount())

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, "idx_name", r2.Name())
		assert.Equal(t, IndexId(2), r2.IndexId())
		assert.Equal(t, IndexTypeNonUnique, r2.IndexType())
		assert.Equal(t, 2, r2.ColumnCount())

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("空のメタデータの場合は false を返す", func(t *testing.T) {
		// GIVEN
		im, bp := setupTestIndexMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		iter, err := im.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)
		defer iter.Close()

		// WHEN
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}
