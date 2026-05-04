package catalog

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewCatalog(t *testing.T) {
	t.Run("HeapFile が未登録の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		bp := buffer.NewBufferPool(page.PageSize * 20)

		// WHEN
		_, err := NewCatalog(bp)

		// THEN
		assert.Error(t, err)
	})

	t.Run("CreateCatalog で作成したカタログを開ける", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		_, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// WHEN
		catalog, err := NewCatalog(bp)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, catalog)
		assert.Equal(t, page.FileId(2), catalog.nextFileId)
		assert.Equal(t, IndexId(0), catalog.nextIndexId)
		assert.Equal(t, page.FileId(1), catalog.UndoLogFileId)
	})

	t.Run("6 つのメタデータのページ ID が復元される", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		created, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// WHEN
		opened, err := NewCatalog(bp)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, created.TableMeta.tree.MetaPageId, opened.TableMeta.tree.MetaPageId)
		assert.Equal(t, created.IndexMeta.tree.MetaPageId, opened.IndexMeta.tree.MetaPageId)
		assert.Equal(t, created.IndexKeyColMeta.tree.MetaPageId, opened.IndexKeyColMeta.tree.MetaPageId)
		assert.Equal(t, created.ColumnMeta.tree.MetaPageId, opened.ColumnMeta.tree.MetaPageId)
		assert.Equal(t, created.ConstraintMeta.tree.MetaPageId, opened.ConstraintMeta.tree.MetaPageId)
		assert.Equal(t, created.UserMeta.tree.MetaPageId, opened.UserMeta.tree.MetaPageId)
	})

	t.Run("マジックナンバーが不正な場合 ErrInvalidCatalogFile を返す", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		_, err := CreateCatalog(bp)
		assert.NoError(t, err)

		headerPageId := page.NewPageId(catalogFileId, catalogHeaderPageNum)
		pageHeader, err := bp.GetWritePage(headerPageId)
		assert.NoError(t, err)
		copy(pageHeader.Body[headerMagicNumberOffset:], []byte("XXXX"))
		bp.UnRefPage(headerPageId)

		// WHEN
		_, err = NewCatalog(bp)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidCatalogFile)
	})
}

func TestCreateCatalog(t *testing.T) {
	t.Run("HeapFile が未登録の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		bp := buffer.NewBufferPool(page.PageSize * 20)

		// WHEN
		_, err := CreateCatalog(bp)

		// THEN
		assert.Error(t, err)
	})

	t.Run("カタログを新規作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)

		// WHEN
		catalog, err := CreateCatalog(bp)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, catalog)
	})

	t.Run("ヘッダーページにマジックナンバーが書き込まれる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)

		// WHEN
		_, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// THEN
		headerPageId := page.NewPageId(catalogFileId, catalogHeaderPageNum)
		pageHeader, err := bp.GetReadPage(headerPageId)
		assert.NoError(t, err)
		defer bp.UnRefPage(headerPageId)

		magicEnd := headerMagicNumberOffset + len(catalogMagicNumber)
		assert.Equal(t, catalogMagicNumber, pageHeader.Body[headerMagicNumberOffset:magicEnd])
	})

	t.Run("ヘッダーページにスカラー値が正しく書き込まれる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)

		// WHEN
		_, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// THEN
		headerPageId := page.NewPageId(catalogFileId, catalogHeaderPageNum)
		pageHeader, err := bp.GetReadPage(headerPageId)
		assert.NoError(t, err)
		defer bp.UnRefPage(headerPageId)

		nextFileId := page.FileId(binary.BigEndian.Uint32(pageHeader.Body[headerNextFileIdOffset : headerNextFileIdOffset+headerFieldSize]))
		nextIndexId := IndexId(binary.BigEndian.Uint32(pageHeader.Body[headerNextIndexIdOffset : headerNextIndexIdOffset+headerFieldSize]))
		undoLogFileId := page.FileId(binary.BigEndian.Uint32(pageHeader.Body[headerUndoLogFileIdOffset : headerUndoLogFileIdOffset+headerFieldSize]))

		assert.Equal(t, page.FileId(2), nextFileId)
		assert.Equal(t, IndexId(0), nextIndexId)
		assert.Equal(t, page.FileId(1), undoLogFileId)
	})

	t.Run("6 つのメタデータが初期化される", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)

		// WHEN
		catalog, err := CreateCatalog(bp)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, catalog.TableMeta)
		assert.NotNil(t, catalog.IndexMeta)
		assert.NotNil(t, catalog.IndexKeyColMeta)
		assert.NotNil(t, catalog.ColumnMeta)
		assert.NotNil(t, catalog.ConstraintMeta)
		assert.NotNil(t, catalog.UserMeta)
	})

	t.Run("各メタデータの metaPageId が有効な値になる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)

		// WHEN
		catalog, err := CreateCatalog(bp)

		// THEN
		assert.NoError(t, err)
		assert.False(t, catalog.TableMeta.tree.MetaPageId.IsInvalid())
		assert.False(t, catalog.IndexMeta.tree.MetaPageId.IsInvalid())
		assert.False(t, catalog.IndexKeyColMeta.tree.MetaPageId.IsInvalid())
		assert.False(t, catalog.ColumnMeta.tree.MetaPageId.IsInvalid())
		assert.False(t, catalog.ConstraintMeta.tree.MetaPageId.IsInvalid())
		assert.False(t, catalog.UserMeta.tree.MetaPageId.IsInvalid())
	})
}

func TestAllocateFileId(t *testing.T) {
	t.Run("FileId を採番するたびにインクリメントされる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		ct, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// WHEN
		id1, err := ct.AllocateFileId()
		assert.NoError(t, err)
		id2, err := ct.AllocateFileId()
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, page.FileId(2), id1)
		assert.Equal(t, page.FileId(3), id2)
	})
}

func TestAllocateIndexId(t *testing.T) {
	t.Run("IndexId を採番するたびにインクリメントされる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		ct, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// WHEN
		id1, err := ct.AllocateIndexId()
		assert.NoError(t, err)
		id2, err := ct.AllocateIndexId()
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, IndexId(0), id1)
		assert.Equal(t, IndexId(1), id2)
	})
}

// setupCatalogTestBufferPool はカタログテスト用のバッファプールを作成する
func setupCatalogTestBufferPool(t *testing.T) *buffer.BufferPool {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog_test.db")
	fileId := page.FileId(0)
	hf, err := file.NewHeapFile(fileId, path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = hf.Close() })
	bp := buffer.NewBufferPool(page.PageSize * 20)
	bp.RegisterHeapFile(fileId, hf)
	return bp
}
