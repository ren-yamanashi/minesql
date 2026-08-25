package buffer

import (
	"path/filepath"
	"testing"

	"github.com/ncw/directio"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewPool(t *testing.T) {
	t.Run("サイズが PageSize 以下の場合 maxPages が 1 になる", func(t *testing.T) {
		// GIVEN / WHEN
		bp := NewPool(page.Size, newTestRedoLog(t), nil)

		// THEN
		assert.Equal(t, 1, bp.maxPages)
	})

	t.Run("サイズが PageSize より大きい場合 maxPages が算出される", func(t *testing.T) {
		// GIVEN / WHEN
		bp := NewPool(page.Size*3, newTestRedoLog(t), nil)

		// THEN
		assert.Equal(t, 3, bp.maxPages)
	})

	t.Run("サイズが 0 の場合 maxPages が 1 になる", func(t *testing.T) {
		// GIVEN / WHEN
		bp := NewPool(0, newTestRedoLog(t), nil)

		// THEN
		assert.Equal(t, 1, bp.maxPages)
	})
}

func TestPage(t *testing.T) {
	t.Run("取得した直後はダーティーにならない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		bufPage, err := bp.Page(pageId)

		// THEN
		assert.NoError(t, err)
		assert.False(t, bufPage.isDirty)
	})

	t.Run("キャッシュ済みのページを取得できる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		bufPage, err := bp.Page(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, pageId, bufPage.pageId)
	})

	t.Run("キャッシュにないページをディスクから読み込める", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		writePageToDisk(t, hf, 0, 0xAB)

		// WHEN
		pageId := page.NewId(0, 0)
		bufPage, err := bp.Page(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(0xAB), bufPage.data.Body()[0])
	})

	t.Run("同じページを 2 回フェッチしても同じデータが返る", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		pageId := page.NewId(0, 0)
		addedPage, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		addedPage.WriteBodyAt(0, []byte{0x42})

		// WHEN
		bufPage1, err := bp.Page(pageId)
		assert.NoError(t, err)
		bufPage2, err := bp.Page(pageId)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, byte(0x42), bufPage1.data.Body()[0])
		assert.Equal(t, byte(0x42), bufPage2.data.Body()[0])
	})

	t.Run("呼び出すと pinCount がインクリメントされる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		bufPage, err := bp.Page(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, bufPage.pinCount)
	})

	t.Run("MarkModified を複数回呼んでもフラッシュリストに重複追加されない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.Page(pageId)
		assert.NoError(t, err)

		// WHEN
		p.MarkModified()
		p.MarkModified()

		// THEN
		assert.Equal(t, 1, bp.flushList.pageCount)
	})
}

func TestUnpin(t *testing.T) {
	t.Run("Unpin 後に pinCount が減る", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		bufPage, err := bp.Page(pageId)
		assert.NoError(t, err)
		assert.Equal(t, 1, bufPage.pinCount)

		// WHEN
		bp.Unpin(pageId)

		// THEN
		bufId, _ := bp.pageTable.bufferId(pageId)
		assert.Equal(t, 0, bp.pages[bufId].pinCount)
	})

	t.Run("複数回 Pin したページは Unpin と同数の解放が必要", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		_, err = bp.Page(pageId)
		assert.NoError(t, err)
		_, err = bp.Page(pageId)
		assert.NoError(t, err)
		bufId, _ := bp.pageTable.bufferId(pageId)
		assert.Equal(t, 2, bp.pages[bufId].pinCount)

		// WHEN
		bp.Unpin(pageId)

		// THEN
		assert.Equal(t, 1, bp.pages[bufId].pinCount)
	})

	t.Run("キャッシュにないページを Unpin しても何も起きない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN / THEN (panic しない)
		bp.Unpin(page.NewId(0, 99))
		_, cached := bp.pageTable.bufferId(pageId)
		assert.True(t, cached)
	})
}

func TestRegisterHeapFile(t *testing.T) {
	t.Run("HeapFile を登録すると取得できる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 1)

		// WHEN
		bp.RegisterHeapFile(1, hf)

		// THEN
		got, err := bp.heapFile(1)
		assert.NoError(t, err)
		assert.Equal(t, hf, got)
	})
}

func TestHasHeapFile(t *testing.T) {
	t.Run("登録されている FileId に対しては true を返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 5)
		bp.RegisterHeapFile(5, hf)

		// WHEN
		ok := bp.HasHeapFile(5)

		// THEN
		assert.True(t, ok)
	})

	t.Run("登録されていない FileId に対しては false を返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size, newTestRedoLog(t), nil)

		// WHEN
		ok := bp.HasHeapFile(99)

		// THEN
		assert.False(t, ok)
	})

	t.Run("DeleteFile 後の FileId に対しては false を返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 5)
		bp.RegisterHeapFile(5, hf)
		assert.NoError(t, bp.DeleteFile(5, 0, nil))

		// WHEN
		ok := bp.HasHeapFile(5)

		// THEN
		assert.False(t, ok)
	})
}

func TestMaxPages(t *testing.T) {
	t.Run("バッファプールの最大ページ数を返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, newTestRedoLog(t), nil)

		// WHEN
		result := bp.MaxPages()

		// THEN
		assert.Equal(t, 3, result)
	})

	t.Run("最小サイズの場合 1 を返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(0, newTestRedoLog(t), nil)

		// WHEN
		result := bp.MaxPages()

		// THEN
		assert.Equal(t, 1, result)
	})
}

func TestFlushListPageCount(t *testing.T) {
	t.Run("ダーティーページの数を返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, newTestRedoLog(t), nil)
		_, err := bp.AddPage(page.NewId(0, 0))
		assert.NoError(t, err)
		_, err = bp.AddPage(page.NewId(0, 1))
		assert.NoError(t, err)
		p0, err := bp.Page(page.NewId(0, 0))
		assert.NoError(t, err)
		p0.MarkModified()
		p1, err := bp.Page(page.NewId(0, 1))
		assert.NoError(t, err)
		p1.MarkModified()

		// WHEN
		size := bp.FlushListPageCount()

		// THEN
		assert.Equal(t, 2, size)
	})

	t.Run("ダーティーページがない場合 0 を返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size, newTestRedoLog(t), nil)

		// WHEN
		size := bp.FlushListPageCount()

		// THEN
		assert.Equal(t, 0, size)
	})
}

func TestForEachDirtyPage(t *testing.T) {
	t.Run("ダーティーページごとにコールバックが実行される", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		id0 := page.NewId(0, 0)
		id1 := page.NewId(0, 1)
		_, _ = bp.AddPage(id0)
		_, _ = bp.AddPage(id1)
		p0, _ := bp.Page(id0)
		p0.data.Body()[0] = 0xAA
		p0.MarkModified()
		p1, _ := bp.Page(id1)
		p1.data.Body()[0] = 0xBB
		p1.MarkModified()

		// WHEN
		var pages []*page.Page
		bp.ForEachDirtyPage(func(pg *page.Page) {
			pages = append(pages, pg)
		})

		// THEN
		assert.Len(t, pages, 2)
	})

	t.Run("フラッシュリストが空の場合コールバックが呼ばれない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)

		// WHEN
		called := false
		bp.ForEachDirtyPage(func(pg *page.Page) {
			called = true
		})

		// THEN
		assert.False(t, called)
	})

	t.Run("コールバック内でページの Header を読み取れる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, _ = bp.AddPage(pageId)
		p, _ := bp.Page(pageId)
		p.data.Header()[0] = 0x12
		p.data.Header()[1] = 0x34
		p.MarkModified()

		// WHEN
		var header []byte
		bp.ForEachDirtyPage(func(pg *page.Page) {
			header = pg.Header()
		})

		// THEN
		assert.Equal(t, byte(0x12), header[0])
		assert.Equal(t, byte(0x34), header[1])
	})
}

const (
	concurrentWorkers    = 4
	concurrentIterations = 50
)

func setupHeapFile(t *testing.T, fileId page.FileId) *file.HeapFile {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	hf, err := file.NewHeapFile(path)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	return hf
}

func writePageToDisk(t *testing.T, hf *file.HeapFile, pageNum page.PageNumber, value byte) {
	t.Helper()
	data := directio.AlignedBlock(page.Size)
	data[page.HeaderSize] = value
	err := hf.Write(pageNum, data)
	assert.NoError(t, err)
}
