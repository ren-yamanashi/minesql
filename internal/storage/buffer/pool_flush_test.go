package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestFlushAllPages(t *testing.T) {
	t.Run("ダーティーページがディスクに書き出されクリーンになる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 2)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.PageForWrite(pageId)
		assert.NoError(t, err)
		p.data.Body[0] = 0xAA

		// WHEN
		err = bp.FlushAllPages()

		// THEN
		assert.NoError(t, err)
		bufPage, err := bp.PageForRead(pageId)
		assert.NoError(t, err)
		assert.False(t, bufPage.isDirty)
	})

	t.Run("フラッシュ後にフラッシュリストがクリアされる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 2)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		_, err = bp.PageForWrite(pageId)
		assert.NoError(t, err)

		// WHEN
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, 0, bp.FlushListPageCount())
	})

	t.Run("フラッシュ後にデータがディスクに永続化されている", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size) // MaxNumOfPage=1
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.PageForWrite(pageId)
		assert.NoError(t, err)
		p.data.Body[0] = 0xBB
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// WHEN
		otherId := page.NewId(0, 1)
		_, err = bp.AddPage(otherId)
		assert.NoError(t, err)
		reloaded, err := bp.PageForRead(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(0xBB), reloaded.data.Body[0])
	})

	t.Run("ダーティーページがない場合もエラーにならない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 2)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)

		// WHEN
		err := bp.FlushAllPages()

		// THEN
		assert.NoError(t, err)
	})
}

func TestFlushOldestPages(t *testing.T) {
	t.Run("指定した件数のダーティーページをフラッシュする", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 3)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		id0 := page.NewId(0, 0)
		id1 := page.NewId(0, 1)
		_, err := bp.AddPage(id0)
		assert.NoError(t, err)
		_, err = bp.AddPage(id1)
		assert.NoError(t, err)
		_, err = bp.PageForWrite(id0)
		assert.NoError(t, err)
		_, err = bp.PageForWrite(id1)
		assert.NoError(t, err)

		// WHEN
		err = bp.FlushOldestPages(1)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, bp.FlushListPageCount())
	})

	t.Run("フラッシュリストが空の場合何もしない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 2)

		// WHEN
		err := bp.FlushOldestPages(10)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("フラッシュしたページがクリーンになる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 2)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		_, err = bp.PageForWrite(pageId)
		assert.NoError(t, err)

		// WHEN
		err = bp.FlushOldestPages(1)
		assert.NoError(t, err)

		// THEN
		bufPage, err := bp.PageForRead(pageId)
		assert.NoError(t, err)
		assert.False(t, bufPage.isDirty)
	})
}

func TestFlushListPageCount(t *testing.T) {
	t.Run("ダーティーページの数を返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 3)
		_, err := bp.AddPage(page.NewId(0, 0))
		assert.NoError(t, err)
		_, err = bp.AddPage(page.NewId(0, 1))
		assert.NoError(t, err)
		_, err = bp.PageForWrite(page.NewId(0, 0))
		assert.NoError(t, err)
		_, err = bp.PageForWrite(page.NewId(0, 1))
		assert.NoError(t, err)

		// WHEN
		size := bp.FlushListPageCount()

		// THEN
		assert.Equal(t, 2, size)
	})

	t.Run("ダーティーページがない場合 0 を返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size)

		// WHEN
		size := bp.FlushListPageCount()

		// THEN
		assert.Equal(t, 0, size)
	})
}

func TestForEachDirtyPage(t *testing.T) {
	t.Run("ダーティーページごとにコールバックが実行される", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 3)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		id0 := page.NewId(0, 0)
		id1 := page.NewId(0, 1)
		_, _ = bp.AddPage(id0)
		_, _ = bp.AddPage(id1)
		p0, _ := bp.PageForWrite(id0)
		p0.data.Body[0] = 0xAA
		p1, _ := bp.PageForWrite(id1)
		p1.data.Body[0] = 0xBB

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
		bp := NewPool(page.Size * 2)

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
		bp := NewPool(page.Size * 2)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, _ = bp.AddPage(pageId)
		p, _ := bp.PageForWrite(pageId)
		p.data.Header[0] = 0x12
		p.data.Header[1] = 0x34

		// WHEN
		var header []byte
		bp.ForEachDirtyPage(func(pg *page.Page) {
			header = pg.Header
		})

		// THEN
		assert.Equal(t, byte(0x12), header[0])
		assert.Equal(t, byte(0x34), header[1])
	})
}
