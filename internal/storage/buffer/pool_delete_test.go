package buffer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

func TestDeleteFile(t *testing.T) {
	t.Run("登録済み FileId を削除すると物理ファイルが消える", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		fileId := page.FileId(7)
		path := registerDeletableHeapFile(t, bp, fileId)
		pageId := page.NewId(fileId, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		err = bp.DeleteFile(fileId, 0, nil)

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
		err := bp.DeleteFile(fileId, 0, nil)

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
		pageId := page.NewId(fileId, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		bufPage, err := bp.Page(pageId)
		assert.NoError(t, err)
		bufPage.WriteBodyAt(0, []byte{0x42})
		assert.Equal(t, 1, bp.flushList.pageCount)

		// WHEN
		err = bp.DeleteFile(fileId, 0, nil)

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
		err := bp.DeleteFile(fileId, 0, nil)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("未登録の FileId に対する DeleteFile は no-op で nil を返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)

		// WHEN
		err := bp.DeleteFile(page.FileId(99), 0, nil)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("DeleteFile を 2 回連続で呼んでもエラーにならない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		fileId := page.FileId(7)
		registerDeletableHeapFile(t, bp, fileId)

		// WHEN
		firstErr := bp.DeleteFile(fileId, 0, nil)
		secondErr := bp.DeleteFile(fileId, 0, nil)

		// THEN
		assert.NoError(t, firstErr)
		assert.NoError(t, secondErr)
	})

	t.Run("削除済み FileId に対する Page はエラーを返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		fileId := page.FileId(7)
		registerDeletableHeapFile(t, bp, fileId)
		pageId := page.NewId(fileId, 0)
		assert.NoError(t, bp.DeleteFile(fileId, 0, nil))

		// WHEN
		_, err := bp.Page(pageId)

		// THEN
		assert.Error(t, err)
	})

	t.Run("削除後にバッファスロットが再利用される", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		droppedFileId := page.FileId(7)
		registerDeletableHeapFile(t, bp, droppedFileId)
		droppedPageId := page.NewId(droppedFileId, 0)
		_, err := bp.AddPage(droppedPageId)
		assert.NoError(t, err)
		assert.NoError(t, bp.DeleteFile(droppedFileId, 0, nil))

		// WHEN
		newFileId := page.FileId(8)
		registerDeletableHeapFile(t, bp, newFileId)
		newPageId := page.NewId(newFileId, 0)
		_, err = bp.AddPage(newPageId)
		assert.NoError(t, err)

		// THEN
		_, exists := bp.pageTable.bufferId(newPageId)
		assert.True(t, exists)
	})

	t.Run("redoLog 付き削除で FileDelete レコードが記録される", func(t *testing.T) {
		// GIVEN
		redoLog := newTestRedoLog(t)
		bp := NewPool(page.Size*2, redoLog, nil)
		fileId := page.FileId(7)
		registerDeletableHeapFile(t, bp, fileId)
		trxId := lock.TrxId(42)

		// WHEN
		err := bp.DeleteFile(fileId, trxId, redoLog)

		// THEN
		assert.NoError(t, err)
		records, err := redoLog.ReadFrom(redo.Lsn(0))
		assert.NoError(t, err)
		var found bool
		for _, rec := range records {
			if rec.Type() == redo.RecordTypeFileDelete && rec.PageId().FileId() == fileId && rec.TrxId() == trxId {
				found = true
				break
			}
		}
		assert.True(t, found, "FileDelete レコードが redo に記録されていない")
	})

	t.Run("未登録 FileId への redoLog 付き削除では FileDelete レコードが記録されない", func(t *testing.T) {
		// GIVEN
		redoLog := newTestRedoLog(t)
		bp := NewPool(page.Size*2, redoLog, nil)
		trxId := lock.TrxId(42)

		// WHEN
		err := bp.DeleteFile(page.FileId(99), trxId, redoLog)

		// THEN
		assert.NoError(t, err)
		records, err := redoLog.ReadFrom(redo.Lsn(0))
		assert.NoError(t, err)
		for _, rec := range records {
			assert.NotEqual(t, redo.RecordTypeFileDelete, rec.Type())
		}
	})
}

func registerDeletableHeapFile(t *testing.T, bp *Pool, fileId page.FileId) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "delete_test.db")
	hf, err := file.NewHeapFile(path)
	assert.NoError(t, err)
	bp.RegisterHeapFile(fileId, hf)
	return path
}
