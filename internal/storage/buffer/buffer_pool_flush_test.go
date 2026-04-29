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
		pageId := page.NewPageId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.GetWritePageData(pageId)
		assert.NoError(t, err)
		p.Body[0] = 0xAA

		// WHEN
		err = bp.FlushAllPages()

		// THEN
		assert.NoError(t, err)
		bufPage, err := bp.FetchPage(pageId)
		assert.NoError(t, err)
		assert.False(t, bufPage.IsDirty)
	})

	t.Run("フラッシュ後にフラッシュリストがクリアされる", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewPageId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		_, err = bp.GetWritePageData(pageId)
		assert.NoError(t, err)

		// WHEN
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, uint32(0), bp.FlushListSize())
	})

	t.Run("フラッシュ後にデータがディスクに永続化されている", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize) // MaxNumOfPage=1
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewPageId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.GetWritePageData(pageId)
		assert.NoError(t, err)
		p.Body[0] = 0xBB
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// WHEN
		otherId := page.NewPageId(0, 1)
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
		id0 := page.NewPageId(0, 0)
		id1 := page.NewPageId(0, 1)
		_, err := bp.AddPage(id0)
		assert.NoError(t, err)
		_, err = bp.AddPage(id1)
		assert.NoError(t, err)
		_, err = bp.GetWritePageData(id0)
		assert.NoError(t, err)
		_, err = bp.GetWritePageData(id1)
		assert.NoError(t, err)

		// WHEN
		err = bp.FlushOldestPages(1)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint32(1), bp.FlushListSize())
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
		pageId := page.NewPageId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		_, err = bp.GetWritePageData(pageId)
		assert.NoError(t, err)

		// WHEN
		err = bp.FlushOldestPages(1)
		assert.NoError(t, err)

		// THEN
		bufPage, err := bp.FetchPage(pageId)
		assert.NoError(t, err)
		assert.False(t, bufPage.IsDirty)
	})
}

func TestFlushListSize(t *testing.T) {
	t.Run("ダーティーページの数を返す", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 3)
		_, err := bp.AddPage(page.NewPageId(0, 0))
		assert.NoError(t, err)
		_, err = bp.AddPage(page.NewPageId(0, 1))
		assert.NoError(t, err)
		_, err = bp.GetWritePageData(page.NewPageId(0, 0))
		assert.NoError(t, err)
		_, err = bp.GetWritePageData(page.NewPageId(0, 1))
		assert.NoError(t, err)

		// WHEN
		size := bp.FlushListSize()

		// THEN
		assert.Equal(t, uint32(2), size)
	})

	t.Run("ダーティーページがない場合 0 を返す", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize)

		// WHEN
		size := bp.FlushListSize()

		// THEN
		assert.Equal(t, uint32(0), size)
	})
}
