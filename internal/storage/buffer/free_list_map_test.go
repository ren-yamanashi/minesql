package buffer

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

func TestInitializeFreeListMapPage(t *testing.T) {
	t.Run("初期化直後のエントリ数は 0", func(t *testing.T) {
		// GIVEN
		bp, _ := setupFreeListMapTestPool(t)
		bufPage := allocateFreshPage(t, bp, page.FileId(0))

		// WHEN
		InitializeFreeListMapPage(bufPage)

		// THEN
		mp := newFreeListMapPage(bufPage)
		assert.Equal(t, uint32(0), mp.entryCount())
	})

	t.Run("初期化後はどの FileId に対しても無効値を返す", func(t *testing.T) {
		// GIVEN
		bp, _ := setupFreeListMapTestPool(t)
		bufPage := allocateFreshPage(t, bp, page.FileId(0))

		// WHEN
		InitializeFreeListMapPage(bufPage)

		// THEN
		mp := newFreeListMapPage(bufPage)
		assert.Equal(t, page.MaxPageNumber, mp.headPageNumber(page.FileId(1)))
		assert.Equal(t, page.MaxPageNumber, mp.headPageNumber(page.FileId(2)))
	})
}

func TestFreeListMapPageHeadPageNumber(t *testing.T) {
	t.Run("登録済みの FileId に対して設定済みの先頭 PageNumber を返す", func(t *testing.T) {
		// GIVEN
		bp, _ := setupFreeListMapTestPool(t)
		bufPage := allocateFreshPage(t, bp, page.FileId(0))
		InitializeFreeListMapPage(bufPage)
		mp := newFreeListMapPage(bufPage)
		mp.setHeadPageNumber(page.FileId(7), page.PageNumber(42))

		// WHEN
		got := mp.headPageNumber(page.FileId(7))

		// THEN
		assert.Equal(t, page.PageNumber(42), got)
	})

	t.Run("未登録の FileId に対しては無効値を返す", func(t *testing.T) {
		// GIVEN
		bp, _ := setupFreeListMapTestPool(t)
		bufPage := allocateFreshPage(t, bp, page.FileId(0))
		InitializeFreeListMapPage(bufPage)
		mp := newFreeListMapPage(bufPage)
		mp.setHeadPageNumber(page.FileId(7), page.PageNumber(42))

		// WHEN
		got := mp.headPageNumber(page.FileId(8))

		// THEN
		assert.Equal(t, page.MaxPageNumber, got)
	})
}

func TestFreeListMapPageSetHeadPageNumber(t *testing.T) {
	t.Run("新規 FileId のエントリを追加できる", func(t *testing.T) {
		// GIVEN
		bp, _ := setupFreeListMapTestPool(t)
		bufPage := allocateFreshPage(t, bp, page.FileId(0))
		InitializeFreeListMapPage(bufPage)
		mp := newFreeListMapPage(bufPage)

		// WHEN
		mp.setHeadPageNumber(page.FileId(3), page.PageNumber(100))

		// THEN
		assert.Equal(t, uint32(1), mp.entryCount())
		assert.Equal(t, page.PageNumber(100), mp.headPageNumber(page.FileId(3)))
	})

	t.Run("既存 FileId のエントリを上書きできる", func(t *testing.T) {
		// GIVEN
		bp, _ := setupFreeListMapTestPool(t)
		bufPage := allocateFreshPage(t, bp, page.FileId(0))
		InitializeFreeListMapPage(bufPage)
		mp := newFreeListMapPage(bufPage)
		mp.setHeadPageNumber(page.FileId(3), page.PageNumber(100))

		// WHEN
		mp.setHeadPageNumber(page.FileId(3), page.PageNumber(200))

		// THEN
		assert.Equal(t, uint32(1), mp.entryCount())
		assert.Equal(t, page.PageNumber(200), mp.headPageNumber(page.FileId(3)))
	})

	t.Run("複数 FileId を独立に管理できる", func(t *testing.T) {
		// GIVEN
		bp, _ := setupFreeListMapTestPool(t)
		bufPage := allocateFreshPage(t, bp, page.FileId(0))
		InitializeFreeListMapPage(bufPage)
		mp := newFreeListMapPage(bufPage)

		// WHEN
		mp.setHeadPageNumber(page.FileId(3), page.PageNumber(100))
		mp.setHeadPageNumber(page.FileId(5), page.PageNumber(200))
		mp.setHeadPageNumber(page.FileId(7), page.PageNumber(300))

		// THEN
		assert.Equal(t, uint32(3), mp.entryCount())
		assert.Equal(t, page.PageNumber(100), mp.headPageNumber(page.FileId(3)))
		assert.Equal(t, page.PageNumber(200), mp.headPageNumber(page.FileId(5)))
		assert.Equal(t, page.PageNumber(300), mp.headPageNumber(page.FileId(7)))
	})

	t.Run("更新後はページがダーティーになる", func(t *testing.T) {
		// GIVEN
		bp, _ := setupFreeListMapTestPool(t)
		bufPage := allocateFreshPage(t, bp, page.FileId(0))
		InitializeFreeListMapPage(bufPage)
		before := bufPage.ModifyCount()

		// WHEN
		newFreeListMapPage(bufPage).setHeadPageNumber(page.FileId(3), page.PageNumber(100))

		// THEN
		assert.Greater(t, bufPage.ModifyCount(), before)
	})
}

func setupFreeListMapTestPool(t *testing.T) (*Pool, *redo.Buffer) {
	t.Helper()
	rl := newFreeListMapTestRedoBuffer(t)
	bp := NewPool(page.Size*8, rl, nil)

	path := filepath.Join(t.TempDir(), "freelistmap_test.db")
	hf, err := file.NewHeapFile(path)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	bp.RegisterHeapFile(page.FileId(0), hf)

	return bp, rl
}

func newFreeListMapTestRedoBuffer(t *testing.T) *redo.Buffer {
	t.Helper()
	rl, err := redo.NewBuffer(t.TempDir())
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = rl.Close() })
	return rl
}

func allocateFreshPage(t *testing.T, bp *Pool, fileId page.FileId) *Page {
	t.Helper()
	pageId := page.NewId(fileId, 0)
	_, err := bp.AddPage(pageId)
	assert.NoError(t, err)
	mtr := NewWriteMtr(bp, lock.SystemReservedTrxId, newFreeListMapTestRedoBuffer(t))
	bufPage, err := mtr.PageForWrite(pageId)
	assert.NoError(t, err)
	t.Cleanup(func() { mtr.UnpinAll() })
	return bufPage
}
