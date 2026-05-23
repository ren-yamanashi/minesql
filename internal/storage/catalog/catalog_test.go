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
		bp := buffer.NewPool(page.Size * 20)

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
		assert.Equal(t, page.FileId(1), catalog.undoLogFileId)
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
		assert.Equal(t, created.tableMeta.tree.MetaPageId(), opened.tableMeta.tree.MetaPageId())
		assert.Equal(t, created.indexMeta.tree.MetaPageId(), opened.indexMeta.tree.MetaPageId())
		assert.Equal(t, created.indexKeyColumnMeta.tree.MetaPageId(), opened.indexKeyColumnMeta.tree.MetaPageId())
		assert.Equal(t, created.columnMeta.tree.MetaPageId(), opened.columnMeta.tree.MetaPageId())
		assert.Equal(t, created.constraintMeta.tree.MetaPageId(), opened.constraintMeta.tree.MetaPageId())
		assert.Equal(t, created.userMeta.tree.MetaPageId(), opened.userMeta.tree.MetaPageId())
	})

	t.Run("マジックナンバーが不正な場合 errInvalidCatalogFile を返す", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		_, err := CreateCatalog(bp)
		assert.NoError(t, err)

		headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
		bufPageHeader, err := bp.PageForWrite(headerPageId)
		assert.NoError(t, err)
		copy(bufPageHeader.Page.Body[headerMagicNumberOffset:], []byte("XXXX"))
		bp.UnRefPage(headerPageId)

		// WHEN
		_, err = NewCatalog(bp)

		// THEN
		assert.ErrorIs(t, err, errInvalidCatalogFile)
	})
}

func TestCreateCatalog(t *testing.T) {
	t.Run("HeapFile が未登録の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		bp := buffer.NewPool(page.Size * 20)

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
		headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
		bufPageHeader, err := bp.PageForRead(headerPageId)
		assert.NoError(t, err)
		defer bp.UnRefPage(headerPageId)

		magicEnd := headerMagicNumberOffset + len(catalogMagicNumber)
		assert.Equal(t, catalogMagicNumber, bufPageHeader.Page.Body[headerMagicNumberOffset:magicEnd])
	})

	t.Run("ヘッダーページにスカラー値が正しく書き込まれる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)

		// WHEN
		_, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// THEN
		headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
		bufPageHeader, err := bp.PageForRead(headerPageId)
		assert.NoError(t, err)
		defer bp.UnRefPage(headerPageId)

		nextFileId := page.FileId(binary.BigEndian.Uint32(bufPageHeader.Page.Body[headerNextFileIdOffset : headerNextFileIdOffset+headerFieldSize]))
		nextIndexId := IndexId(binary.BigEndian.Uint32(bufPageHeader.Page.Body[headerNextIndexIdOffset : headerNextIndexIdOffset+headerFieldSize]))
		undoLogFileId := page.FileId(binary.BigEndian.Uint32(bufPageHeader.Page.Body[headerUndoLogFileIdOffset : headerUndoLogFileIdOffset+headerFieldSize]))

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
		assert.NotNil(t, catalog.tableMeta)
		assert.NotNil(t, catalog.indexMeta)
		assert.NotNil(t, catalog.indexKeyColumnMeta)
		assert.NotNil(t, catalog.columnMeta)
		assert.NotNil(t, catalog.constraintMeta)
		assert.NotNil(t, catalog.userMeta)
	})

	t.Run("各メタデータの metaPageId が有効な値になる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)

		// WHEN
		catalog, err := CreateCatalog(bp)

		// THEN
		assert.NoError(t, err)
		assert.False(t, catalog.tableMeta.tree.MetaPageId().IsInvalid())
		assert.False(t, catalog.indexMeta.tree.MetaPageId().IsInvalid())
		assert.False(t, catalog.indexKeyColumnMeta.tree.MetaPageId().IsInvalid())
		assert.False(t, catalog.columnMeta.tree.MetaPageId().IsInvalid())
		assert.False(t, catalog.constraintMeta.tree.MetaPageId().IsInvalid())
		assert.False(t, catalog.userMeta.tree.MetaPageId().IsInvalid())
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
func setupCatalogTestBufferPool(t *testing.T) *buffer.Pool {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog_test.db")
	fileId := page.FileId(0)
	hf, err := file.NewHeapFile(fileId, path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = hf.Close() })
	bp := buffer.NewPool(page.Size * 20)
	bp.RegisterHeapFile(fileId, hf)
	return bp
}
