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
	t.Run("サイズが PageSize 以下の場合 maxNumOfPage が 1 になる", func(t *testing.T) {
		// GIVEN / WHEN
		bp := NewPool(page.Size)

		// THEN
		assert.Equal(t, 1, bp.maxPages)
	})

	t.Run("サイズが PageSize より大きい場合 maxNumOfPage が算出される", func(t *testing.T) {
		// GIVEN / WHEN
		bp := NewPool(page.Size * 3)

		// THEN
		assert.Equal(t, 3, bp.maxPages)
	})

	t.Run("サイズが 0 の場合 maxNumOfPage が 1 になる", func(t *testing.T) {
		// GIVEN / WHEN
		bp := NewPool(0)

		// THEN
		assert.Equal(t, 1, bp.maxPages)
	})
}

func TestBufferPageForWrite(t *testing.T) {
	t.Run("取得したページがダーティーになる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 2)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		_, err = bp.PageForWrite(pageId)

		// THEN
		assert.NoError(t, err)
		bufPage, err := bp.PageForRead(pageId)
		assert.NoError(t, err)
		assert.True(t, bufPage.isDirty)
	})

	t.Run("既にダーティーなページを再取得してもフラッシュリストに重複追加されない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 2)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		_, err = bp.PageForWrite(pageId)
		assert.NoError(t, err)

		// WHEN
		_, err = bp.PageForWrite(pageId)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, 1, bp.flushList.pageCount)
	})

	t.Run("書き込んだデータがフェッチ時に反映されている", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 2)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		p, err := bp.PageForWrite(pageId)
		assert.NoError(t, err)
		p.data.Body[0] = 0xAA

		// THEN
		fetched, err := bp.PageForRead(pageId)
		assert.NoError(t, err)
		assert.Equal(t, byte(0xAA), fetched.data.Body[0])
	})
}

func TestBufferPageForRead(t *testing.T) {
	t.Run("キャッシュ済みのページを取得できる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 2)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		bufPage, err := bp.PageForRead(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, pageId, bufPage.pageId)
	})

	t.Run("キャッシュにないページをディスクから読み込める", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 2)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		writePageToDisk(t, hf, 0, 0xAB)

		// WHEN
		pageId := page.NewId(0, 0)
		bufPage, err := bp.PageForRead(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(0xAB), bufPage.data.Body[0])
	})

	t.Run("同じページを 2 回フェッチしても同じデータが返る", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 2)
		pageId := page.NewId(0, 0)
		addedPage, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		addedPage.data.Body[0] = 0x42

		// WHEN
		bufPage1, err := bp.PageForRead(pageId)
		assert.NoError(t, err)
		bufPage2, err := bp.PageForRead(pageId)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, byte(0x42), bufPage1.data.Body[0])
		assert.Equal(t, byte(0x42), bufPage2.data.Body[0])
	})
}

func TestUnrefPage(t *testing.T) {
	t.Run("参照解除したページが優先的に追い出される", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 3)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		id0 := page.NewId(0, 0)
		id1 := page.NewId(0, 1)
		id2 := page.NewId(0, 2)
		_, err := bp.AddPage(id0)
		assert.NoError(t, err)
		_, err = bp.AddPage(id1)
		assert.NoError(t, err)
		_, err = bp.AddPage(id2)
		assert.NoError(t, err)

		// WHEN
		bp.UnrefPage(id0)
		newId := page.NewId(0, 3)
		_, err = bp.AddPage(newId)
		assert.NoError(t, err)

		// THEN
		_, cached := bp.pageTable.bufferId(id0)
		assert.False(t, cached)
		_, cached = bp.pageTable.bufferId(newId)
		assert.True(t, cached)
	})

	t.Run("キャッシュにないページを参照解除しても何も起きない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 2)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN / THEN (panic しない)
		bp.UnrefPage(page.NewId(0, 99))
		_, cached := bp.pageTable.bufferId(pageId)
		assert.True(t, cached)
	})
}

func TestAllocatePageId(t *testing.T) {
	t.Run("新しい PageId を割り当てられる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size)
		hf := setupHeapFile(t, 5)
		bp.RegisterHeapFile(5, hf)

		// WHEN
		id, err := bp.AllocatePageId(5)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.FileId(5), id.FileId)
		assert.Equal(t, page.PageNumber(0), id.PageNumber)
	})

	t.Run("連続で割り当てると PageNumber がインクリメントされる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)

		// WHEN
		id1, err := bp.AllocatePageId(0)
		assert.NoError(t, err)
		id2, err := bp.AllocatePageId(0)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, page.PageNumber(0), id1.PageNumber)
		assert.Equal(t, page.PageNumber(1), id2.PageNumber)
	})

	t.Run("未登録の FileId の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size)

		// WHEN
		id, err := bp.AllocatePageId(99)

		// THEN
		assert.Error(t, err)
		assert.Equal(t, page.InvalidId, id)
	})
}

func TestRegisterHeapFile(t *testing.T) {
	t.Run("HeapFile を登録すると取得できる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size)
		hf := setupHeapFile(t, 1)

		// WHEN
		bp.RegisterHeapFile(1, hf)

		// THEN
		got, err := bp.heapFile(1)
		assert.NoError(t, err)
		assert.Equal(t, hf, got)
	})
}

func TestMaxPages(t *testing.T) {
	t.Run("バッファプールの最大ページ数を返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 3)

		// WHEN
		result := bp.MaxPages()

		// THEN
		assert.Equal(t, 3, result)
	})

	t.Run("最小サイズの場合 1 を返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(0)

		// WHEN
		result := bp.MaxPages()

		// THEN
		assert.Equal(t, 1, result)
	})
}

func setupHeapFile(t *testing.T, fileId page.FileId) *file.HeapFile {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	hf, err := file.NewHeapFile(fileId, path)
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
