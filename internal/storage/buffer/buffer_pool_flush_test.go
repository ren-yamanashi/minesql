package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestFlushAllPages(t *testing.T) {
	t.Run("ダーティーページがディスクに書き出されクリーンになる", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.GetWritePage(pageId)
		assert.NoError(t, err)
		p.Body[0] = 0xAA

		// WHEN
		err = bp.FlushAllPages()

		// THEN
		assert.NoError(t, err)
		bufPage, err := bp.FetchPage(pageId)
		assert.NoError(t, err)
		assert.False(t, bufPage.isDirty)
	})

	t.Run("フラッシュ後にフラッシュリストがクリアされる", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		_, err = bp.GetWritePage(pageId)
		assert.NoError(t, err)

		// WHEN
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, 0, bp.NumOfFlushListPage())
	})

	t.Run("フラッシュ後にデータがディスクに永続化されている", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize) // MaxNumOfPage=1
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.GetWritePage(pageId)
		assert.NoError(t, err)
		p.Body[0] = 0xBB
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// WHEN
		otherId := page.NewId(0, 1)
		_, err = bp.AddPage(otherId)
		assert.NoError(t, err)
		reloaded, err := bp.FetchPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(0xBB), reloaded.Page.Body[0])
	})

	t.Run("ダーティーページがない場合もエラーにならない", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
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
		bp := NewBufferPool(page.PageSize * 3)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		id0 := page.NewId(0, 0)
		id1 := page.NewId(0, 1)
		_, err := bp.AddPage(id0)
		assert.NoError(t, err)
		_, err = bp.AddPage(id1)
		assert.NoError(t, err)
		_, err = bp.GetWritePage(id0)
		assert.NoError(t, err)
		_, err = bp.GetWritePage(id1)
		assert.NoError(t, err)

		// WHEN
		err = bp.FlushOldestPages(1)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, bp.NumOfFlushListPage())
	})

	t.Run("フラッシュリストが空の場合何もしない", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)

		// WHEN
		err := bp.FlushOldestPages(10)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("フラッシュしたページがクリーンになる", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		_, err = bp.GetWritePage(pageId)
		assert.NoError(t, err)

		// WHEN
		err = bp.FlushOldestPages(1)
		assert.NoError(t, err)

		// THEN
		bufPage, err := bp.FetchPage(pageId)
		assert.NoError(t, err)
		assert.False(t, bufPage.isDirty)
	})
}

func TestForEachDirtyPage(t *testing.T) {
	t.Run("ダーティーページごとにコールバックが実行される", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 3)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		id0 := page.NewId(0, 0)
		id1 := page.NewId(0, 1)
		_, _ = bp.AddPage(id0)
		_, _ = bp.AddPage(id1)
		p0, _ := bp.GetWritePage(id0)
		p0.Body[0] = 0xAA
		p1, _ := bp.GetWritePage(id1)
		p1.Body[0] = 0xBB

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
		bp := NewBufferPool(page.PageSize * 2)

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
		bp := NewBufferPool(page.PageSize * 2)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, _ = bp.AddPage(pageId)
		p, _ := bp.GetWritePage(pageId)
		p.Header[0] = 0x12
		p.Header[1] = 0x34

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

func TestNumOfFlushListPage(t *testing.T) {
	t.Run("ダーティーページの数を返す", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 3)
		_, err := bp.AddPage(page.NewId(0, 0))
		assert.NoError(t, err)
		_, err = bp.AddPage(page.NewId(0, 1))
		assert.NoError(t, err)
		_, err = bp.GetWritePage(page.NewId(0, 0))
		assert.NoError(t, err)
		_, err = bp.GetWritePage(page.NewId(0, 1))
		assert.NoError(t, err)

		// WHEN
		size := bp.NumOfFlushListPage()

		// THEN
		assert.Equal(t, 2, size)
	})

	t.Run("ダーティーページがない場合 0 を返す", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize)

		// WHEN
		size := bp.NumOfFlushListPage()

		// THEN
		assert.Equal(t, 0, size)
	})
}
