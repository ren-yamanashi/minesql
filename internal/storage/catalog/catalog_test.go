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
		assert.Equal(t, created.TableMeta.metaPageId, opened.TableMeta.metaPageId)
		assert.Equal(t, created.IndexMeta.metaPageId, opened.IndexMeta.metaPageId)
		assert.Equal(t, created.IndexKeyColMeta.metaPageId, opened.IndexKeyColMeta.metaPageId)
		assert.Equal(t, created.ColumnMeta.metaPageId, opened.ColumnMeta.metaPageId)
		assert.Equal(t, created.ConstraintMeta.metaPageId, opened.ConstraintMeta.metaPageId)
		assert.Equal(t, created.UserMeta.metaPageId, opened.UserMeta.metaPageId)
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
		assert.False(t, catalog.TableMeta.metaPageId.IsInvalid())
		assert.False(t, catalog.IndexMeta.metaPageId.IsInvalid())
		assert.False(t, catalog.IndexKeyColMeta.metaPageId.IsInvalid())
		assert.False(t, catalog.ColumnMeta.metaPageId.IsInvalid())
		assert.False(t, catalog.ConstraintMeta.metaPageId.IsInvalid())
		assert.False(t, catalog.UserMeta.metaPageId.IsInvalid())
	})
}

// setupCatalogTestBufferPool はカタログテスト用のバッファプールを作成する
// CreateCatalog は 13 ページ (ヘッダー 1 + B+Tree 6×2) を割り当てるため、十分な容量を確保する
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
