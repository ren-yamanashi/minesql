package buffer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestDeleteFile(t *testing.T) {
	t.Run("登録済み FileId を削除すると物理ファイルが消える", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		fileId := page.FileId(7)
		path := registerDeletableHeapFile(t, bp, fileId)
		pageId, err := bp.AllocatePageId(fileId)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		err = bp.DeleteFile(fileId)

		// THEN
		assert.NoError(t, err)
		_, statErr := os.Stat(path)
		assert.True(t, os.IsNotExist(statErr))
	})

	t.Run("登録済み FileId を削除すると map から取り除かれる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		fileId := page.FileId(7)
		registerDeletableHeapFile(t, bp, fileId)

		// WHEN
		err := bp.DeleteFile(fileId)

		// THEN
		assert.NoError(t, err)
		_, ok := bp.files[fileId]
		assert.False(t, ok)
	})

	t.Run("ダーティページがあっても Flush せず破棄される", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		fileId := page.FileId(7)
		registerDeletableHeapFile(t, bp, fileId)
		pageId, err := bp.AllocatePageId(fileId)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId)
		assert.NoError(t, err)
		bufPage, err := bp.Page(pageId)
		assert.NoError(t, err)
		bufPage.WriteBodyAt(0, []byte{0x42})
		assert.Equal(t, 1, bp.flushList.pageCount)

		// WHEN
		err = bp.DeleteFile(fileId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 0, bp.flushList.pageCount)
		_, exists := bp.pageTable.bufferId(pageId)
		assert.False(t, exists)
	})

	t.Run("削除前に物理ファイルが消えていても nil を返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		fileId := page.FileId(7)
		path := registerDeletableHeapFile(t, bp, fileId)
		assert.NoError(t, os.Remove(path))

		// WHEN
		err := bp.DeleteFile(fileId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("未登録の FileId を削除しようとするとエラーを返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)

		// WHEN
		err := bp.DeleteFile(page.FileId(99))

		// THEN
		assert.Error(t, err)
	})

	t.Run("削除済み FileId に対する AllocatePageId はエラーを返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		fileId := page.FileId(7)
		registerDeletableHeapFile(t, bp, fileId)
		assert.NoError(t, bp.DeleteFile(fileId))

		// WHEN
		_, err := bp.AllocatePageId(fileId)

		// THEN
		assert.Error(t, err)
	})

	t.Run("削除済み FileId に対する Page はエラーを返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		fileId := page.FileId(7)
		registerDeletableHeapFile(t, bp, fileId)
		pageId, err := bp.AllocatePageId(fileId)
		assert.NoError(t, err)
		assert.NoError(t, bp.DeleteFile(fileId))

		// WHEN
		_, err = bp.Page(pageId)

		// THEN
		assert.Error(t, err)
	})

	t.Run("削除後にバッファスロットが再利用される", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		droppedFileId := page.FileId(7)
		registerDeletableHeapFile(t, bp, droppedFileId)
		droppedPageId, err := bp.AllocatePageId(droppedFileId)
		assert.NoError(t, err)
		_, err = bp.AddPage(droppedPageId)
		assert.NoError(t, err)
		assert.NoError(t, bp.DeleteFile(droppedFileId))

		// WHEN
		newFileId := page.FileId(8)
		registerDeletableHeapFile(t, bp, newFileId)
		newPageId, err := bp.AllocatePageId(newFileId)
		assert.NoError(t, err)
		_, err = bp.AddPage(newPageId)
		assert.NoError(t, err)

		// THEN
		_, exists := bp.pageTable.bufferId(newPageId)
		assert.True(t, exists)
	})
}

func registerDeletableHeapFile(t *testing.T, bp *Pool, fileId page.FileId) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "delete_test.db")
	hf, err := file.NewHeapFile(fileId, path)
	assert.NoError(t, err)
	bp.RegisterHeapFile(fileId, hf)
	return path
}
