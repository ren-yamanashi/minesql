package dictionary

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

func TestNewCatalog(t *testing.T) {
	t.Run("HeapFile が未登録の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		bp := buffer.NewPool(page.Size*20, newCatalogTestRedoBuffer(t), nil)

		// WHEN
		_, err := NewCatalog(bp, newCatalogTestRedoBuffer(t))

		// THEN
		assert.Error(t, err)
	})

	t.Run("CreateCatalog で作成したカタログを開ける", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		_, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))
		assert.NoError(t, err)

		// WHEN
		catalog, err := NewCatalog(bp, newCatalogTestRedoBuffer(t))

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, catalog)
		assert.Equal(t, page.FileId(2), catalog.nextFileId)
		assert.Equal(t, IndexId(1), catalog.nextIndexId)
		assert.Equal(t, page.FileId(1), catalog.undoLogFileId)
		assert.False(t, catalog.freeListMapPageId.IsInvalid())
		assert.False(t, catalog.ddlUndoRootPageId.IsInvalid())
	})

	t.Run("CreateCatalog 時の freeListMapPageId を NewCatalog で復元できる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		created, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))
		assert.NoError(t, err)

		// WHEN
		opened, err := NewCatalog(bp, newCatalogTestRedoBuffer(t))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, created.freeListMapPageId, opened.freeListMapPageId)
	})

	t.Run("CreateCatalog 時の ddlUndoRootPageId を NewCatalog で復元できる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		created, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))
		assert.NoError(t, err)

		// WHEN
		opened, err := NewCatalog(bp, newCatalogTestRedoBuffer(t))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, created.ddlUndoRootPageId, opened.ddlUndoRootPageId)
	})

	t.Run("ヘッダーの DDL Undo 先頭 PageNumber が無効値の場合 ddlUndoRootPageId は page.InvalidId() に復元される", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		_, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))
		assert.NoError(t, err)

		headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
		bufPageHeader, err := bp.Page(headerPageId)
		assert.NoError(t, err)
		writePageNumber(bufPageHeader, headerDDLUndoRootPageNumberOffset, page.MaxPageNumber)
		bp.Unpin(headerPageId)

		// WHEN
		opened, err := NewCatalog(bp, newCatalogTestRedoBuffer(t))

		// THEN
		assert.NoError(t, err)
		assert.True(t, opened.ddlUndoRootPageId.IsInvalid())
	})

	t.Run("6 つのメタデータのページ ID が復元される", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		created, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))
		assert.NoError(t, err)

		// WHEN
		opened, err := NewCatalog(bp, newCatalogTestRedoBuffer(t))

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
		_, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))
		assert.NoError(t, err)

		headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
		bufPageHeader, err := bp.Page(headerPageId)
		assert.NoError(t, err)
		bufPageHeader.WriteBodyAt(headerMagicNumberOffset, []byte("XXXX"))
		bp.Unpin(headerPageId)

		// WHEN
		_, err = NewCatalog(bp, newCatalogTestRedoBuffer(t))

		// THEN
		assert.ErrorIs(t, err, errInvalidCatalogFile)
	})
}

func TestCreateCatalog(t *testing.T) {
	t.Run("HeapFile が未登録の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		bp := buffer.NewPool(page.Size*20, newCatalogTestRedoBuffer(t), nil)

		// WHEN
		_, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))

		// THEN
		assert.Error(t, err)
	})

	t.Run("カタログを新規作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)

		// WHEN
		catalog, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, catalog)
	})

	t.Run("ヘッダーページにマジックナンバーが書き込まれる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)

		// WHEN
		_, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))
		assert.NoError(t, err)

		// THEN
		headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
		bufPageHeader, err := bp.Page(headerPageId)
		assert.NoError(t, err)
		defer bp.Unpin(headerPageId)

		magicEnd := headerMagicNumberOffset + len(catalogMagicNumber)
		assert.Equal(t, catalogMagicNumber, bufPageHeader.Data().Body()[headerMagicNumberOffset:magicEnd])
	})

	t.Run("ヘッダーページにスカラー値が正しく書き込まれる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)

		// WHEN
		_, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))
		assert.NoError(t, err)

		// THEN
		headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
		bufPageHeader, err := bp.Page(headerPageId)
		assert.NoError(t, err)
		defer bp.Unpin(headerPageId)

		body := bufPageHeader.Data().Body()
		nextFileIdBytes := body[headerNextFileIdOffset : headerNextFileIdOffset+headerFieldSize]
		nextIndexIdBytes := body[headerNextIndexIdOffset : headerNextIndexIdOffset+headerFieldSize]
		undoLogFileIdBytes := body[headerUndoLogFileIdOffset : headerUndoLogFileIdOffset+headerFieldSize]
		nextFileId := page.FileId(binary.BigEndian.Uint32(nextFileIdBytes))
		nextIndexId := IndexId(binary.BigEndian.Uint32(nextIndexIdBytes))
		undoLogFileId := page.FileId(binary.BigEndian.Uint32(undoLogFileIdBytes))

		assert.Equal(t, page.FileId(2), nextFileId)
		assert.Equal(t, IndexId(1), nextIndexId)
		assert.Equal(t, page.FileId(1), undoLogFileId)
	})

	t.Run("ヘッダーページにフリーリストマップページの PageNumber が書き込まれる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)

		// WHEN
		_, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))
		assert.NoError(t, err)

		// THEN
		headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
		bufPageHeader, err := bp.Page(headerPageId)
		assert.NoError(t, err)
		defer bp.Unpin(headerPageId)

		body := bufPageHeader.Data().Body()
		freeListMapPageNumber := readPageNumber(body, headerFreeListMapPageNumberOffset)
		assert.NotEqual(t, page.MaxPageNumber, freeListMapPageNumber)
	})

	t.Run("ヘッダーページに DDL Undo 先頭ページの PageNumber が書き込まれる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)

		// WHEN
		_, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))
		assert.NoError(t, err)

		// THEN
		headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
		bufPageHeader, err := bp.Page(headerPageId)
		assert.NoError(t, err)
		defer bp.Unpin(headerPageId)

		body := bufPageHeader.Data().Body()
		ddlUndoRootPN := readPageNumber(body, headerDDLUndoRootPageNumberOffset)
		assert.NotEqual(t, page.MaxPageNumber, ddlUndoRootPN)
		assert.NotEqual(t, page.PageNumber(0), ddlUndoRootPN)
	})

	t.Run("6 つのメタデータが初期化される", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)

		// WHEN
		catalog, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))

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
		catalog, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))

		// THEN
		assert.NoError(t, err)
		assert.False(t, catalog.tableMeta.tree.MetaPageId().IsInvalid())
		assert.False(t, catalog.indexMeta.tree.MetaPageId().IsInvalid())
		assert.False(t, catalog.indexKeyColumnMeta.tree.MetaPageId().IsInvalid())
		assert.False(t, catalog.columnMeta.tree.MetaPageId().IsInvalid())
		assert.False(t, catalog.constraintMeta.tree.MetaPageId().IsInvalid())
		assert.False(t, catalog.userMeta.tree.MetaPageId().IsInvalid())
	})

	t.Run("書き込んだヘッダーページがフラッシュ対象になる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)

		// WHEN
		_, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))
		assert.NoError(t, err)

		// THEN
		headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
		bufPageHeader, err := bp.Page(headerPageId)
		assert.NoError(t, err)
		defer bp.Unpin(headerPageId)
		assert.Greater(t, bufPageHeader.ModifyCount(), uint64(0))
	})
}

func TestAllocateIndexId(t *testing.T) {
	t.Run("IndexId を採番するたびにインクリメントされる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		redoLog := newCatalogTestRedoBuffer(t)
		ct, err := CreateCatalog(bp, redoLog)
		assert.NoError(t, err)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
		id1, err := ct.AllocateIndexId(mtr)
		assert.NoError(t, err)
		id2, err := ct.AllocateIndexId(mtr)
		assert.NoError(t, err)
		assert.NoError(t, mtr.Commit())

		// THEN
		assert.Equal(t, IndexId(1), id1)
		assert.Equal(t, IndexId(2), id2)
	})

	t.Run("採番後にヘッダーページがダーティーになる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		redoLog := newCatalogTestRedoBuffer(t)
		ct, err := CreateCatalog(bp, redoLog)
		assert.NoError(t, err)
		headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
		bufPageHeader, err := bp.Page(headerPageId)
		assert.NoError(t, err)
		before := bufPageHeader.ModifyCount()
		bp.Unpin(headerPageId)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
		_, err = ct.AllocateIndexId(mtr)
		assert.NoError(t, err)
		assert.NoError(t, mtr.Commit())

		// THEN
		bufPageHeaderAfter, err := bp.Page(headerPageId)
		assert.NoError(t, err)
		defer bp.Unpin(headerPageId)
		assert.Greater(t, bufPageHeaderAfter.ModifyCount(), before)
	})
}

func TestAllocateFileId(t *testing.T) {
	t.Run("FileId を採番するたびにインクリメントされる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		redoLog := newCatalogTestRedoBuffer(t)
		ct, err := CreateCatalog(bp, redoLog)
		assert.NoError(t, err)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
		id1, err := ct.AllocateFileId(mtr)
		assert.NoError(t, err)
		id2, err := ct.AllocateFileId(mtr)
		assert.NoError(t, err)
		assert.NoError(t, mtr.Commit())

		// THEN
		assert.Equal(t, page.FileId(2), id1)
		assert.Equal(t, page.FileId(3), id2)
	})

	t.Run("採番後にヘッダーページがダーティーになる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		redoLog := newCatalogTestRedoBuffer(t)
		ct, err := CreateCatalog(bp, redoLog)
		assert.NoError(t, err)
		headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
		bufPageHeader, err := bp.Page(headerPageId)
		assert.NoError(t, err)
		before := bufPageHeader.ModifyCount()
		bp.Unpin(headerPageId)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
		_, err = ct.AllocateFileId(mtr)
		assert.NoError(t, err)
		assert.NoError(t, mtr.Commit())

		// THEN
		bufPageHeaderAfter, err := bp.Page(headerPageId)
		assert.NoError(t, err)
		defer bp.Unpin(headerPageId)
		assert.Greater(t, bufPageHeaderAfter.ModifyCount(), before)
	})
}

func TestSetDDLUndoRootPageId(t *testing.T) {
	t.Run("PageId を書き換えるとヘッダーと内部状態が更新される", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		ct, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))
		assert.NoError(t, err)
		newPageId := page.NewId(catalogFileId, page.PageNumber(123))

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newCatalogTestRedoBuffer(t))
		err = ct.SetDDLUndoRootPageId(mtr, newPageId)
		assert.NoError(t, err)
		assert.NoError(t, mtr.Commit())

		// THEN
		assert.Equal(t, newPageId, ct.ddlUndoRootPageId)
		headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
		bufPageHeader, err := bp.Page(headerPageId)
		assert.NoError(t, err)
		defer bp.Unpin(headerPageId)
		pn := readPageNumber(bufPageHeader.Data().Body(), headerDDLUndoRootPageNumberOffset)
		assert.Equal(t, newPageId.PageNumber(), pn)
	})

	t.Run("page.InvalidId() を渡すとヘッダーに無効値が書かれ内部状態も無効化される", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		ct, err := CreateCatalog(bp, newCatalogTestRedoBuffer(t))
		assert.NoError(t, err)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newCatalogTestRedoBuffer(t))
		err = ct.SetDDLUndoRootPageId(mtr, page.InvalidId())
		assert.NoError(t, err)
		assert.NoError(t, mtr.Commit())

		// THEN
		assert.True(t, ct.ddlUndoRootPageId.IsInvalid())
		headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
		bufPageHeader, err := bp.Page(headerPageId)
		assert.NoError(t, err)
		defer bp.Unpin(headerPageId)
		pn := readPageNumber(bufPageHeader.Data().Body(), headerDDLUndoRootPageNumberOffset)
		assert.Equal(t, page.MaxPageNumber, pn)
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
	bp := buffer.NewPool(page.Size*20, newCatalogTestRedoBuffer(t), nil)
	bp.RegisterHeapFile(fileId, hf)
	return bp
}

// newCatalogTestRedoBuffer はカタログテスト用の Redo バッファを作成する
func newCatalogTestRedoBuffer(t *testing.T) *redo.Buffer {
	t.Helper()
	rl, err := redo.NewBuffer(t.TempDir())
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = rl.Close() })
	return rl
}
