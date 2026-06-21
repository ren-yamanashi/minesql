package dictionary

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestCreateIndexMeta(t *testing.T) {
	t.Run("インデックスメタデータを新規作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupDictTestBufferPool(t)
		rl := setupDictTestRedoBuffer(t)

		// WHEN
		im, err := CreateIndexMeta(bp, rl)

		// THEN
		assert.NoError(t, err)
		assert.False(t, im.tree.MetaPageId().IsInvalid())
	})
}

func TestIndexMetaSearch(t *testing.T) {
	t.Run("SearchModeStart で全件スキャンできる", func(t *testing.T) {
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

		// THEN: インデックス名でソートされる
		iter, err := im.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

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

	t.Run("空のメタデータを検索するとレコードが返らない", func(t *testing.T) {
		// GIVEN
		im, bp := setupTestIndexMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()

		// WHEN
		iter, err := im.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestIndexMetaInsert(t *testing.T) {
	t.Run("インデックスメタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		im, bp := setupTestIndexMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()

		// WHEN
		err := im.Insert(mtr, NewIndexMetaRecord(
			page.FileId(1),
			IndexId(1),
			PrimaryIndexName,
			IndexTypePrimary,
			1,
			page.NewId(page.FileId(1), page.PageNumber(0)),
		))

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同じ FileId + インデックス名の重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
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

		// WHEN
		err := im.Insert(mtr, NewIndexMetaRecord(
			page.FileId(1),
			IndexId(2),
			PrimaryIndexName,
			IndexTypePrimary,
			1,
			page.NewId(page.FileId(1), page.PageNumber(0)),
		))

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("異なるインデックス名であれば同じテーブルに複数挿入できる", func(t *testing.T) {
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

		// WHEN
		err := im.Insert(mtr, NewIndexMetaRecord(
			page.FileId(1),
			IndexId(2),
			"idx_email",
			IndexTypeUnique,
			1,
			page.NewId(page.FileId(1), page.PageNumber(0)),
		))

		// THEN
		assert.NoError(t, err)
	})
}

func TestIndexMetaDelete(t *testing.T) {
	t.Run("Insert したレコードを Delete で削除できる", func(t *testing.T) {
		// GIVEN
		im, bp := setupTestIndexMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		record := NewIndexMetaRecord(
			page.FileId(1),
			IndexId(1),
			PrimaryIndexName,
			IndexTypePrimary,
			1,
			page.NewId(page.FileId(1), page.PageNumber(0)),
		)
		_ = im.Insert(mtr, record)

		// WHEN
		err := im.Delete(mtr, record.Encode().Key())

		// THEN
		assert.NoError(t, err)
		iter, err := im.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("存在しないキーの Delete は ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		im, bp := setupTestIndexMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		missing := NewIndexMetaRecord(
			page.FileId(99),
			IndexId(0),
			"missing",
			IndexTypeNonUnique,
			1,
			page.NewId(page.FileId(1), page.PageNumber(0)),
		)

		// WHEN
		err := im.Delete(mtr, missing.Encode().Key())

		// THEN
		assert.ErrorIs(t, err, btree.ErrKeyNotFound)
	})
}

// setupTestIndexMeta はテスト用の IndexMeta を作成する
func setupTestIndexMeta(t *testing.T) (*IndexMeta, *buffer.Pool) {
	t.Helper()
	bp := setupDictTestBufferPool(t)
	im, err := CreateIndexMeta(bp, setupDictTestRedoBuffer(t))
	if err != nil {
		t.Fatalf("IndexMeta の作成に失敗: %v", err)
	}
	return im, bp
}
