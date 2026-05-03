package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestGetWritePage(t *testing.T) {
	t.Run("取得したページがダーティーになる", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		pageId := page.NewPageId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		_, err = bp.GetWritePage(pageId)

		// THEN
		assert.NoError(t, err)
		bufPage, err := bp.FetchPage(pageId)
		assert.NoError(t, err)
		assert.True(t, bufPage.isDirty)
	})

	t.Run("既にダーティーなページを再取得してもフラッシュリストに重複追加されない", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		pageId := page.NewPageId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		_, err = bp.GetWritePage(pageId)
		assert.NoError(t, err)

		// WHEN
		_, err = bp.GetWritePage(pageId)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, uint32(1), bp.flushList.numOfPage)
	})

	t.Run("書き込んだデータがフェッチ時に反映されている", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		pageId := page.NewPageId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		p, err := bp.GetWritePage(pageId)
		assert.NoError(t, err)
		p.Body[0] = 0xAA

		// THEN
		fetched, err := bp.FetchPage(pageId)
		assert.NoError(t, err)
		assert.Equal(t, byte(0xAA), fetched.Page.Body[0])
	})
}

func TestGetReadPage(t *testing.T) {
	t.Run("キャッシュ済みのページを取得できる", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		pageId := page.NewPageId(0, 0)
		addedPage, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		addedPage.Page.Body[0] = 0xCC

		// WHEN
		p, err := bp.GetReadPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(0xCC), p.Body[0])
	})

	t.Run("キャッシュにないページをディスクから読み込める", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		writePageToDisk(t, hf, 0, 0xDD)

		// WHEN
		pageId := page.NewPageId(0, 0)
		p, err := bp.GetReadPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(0xDD), p.Body[0])
	})

	t.Run("読み込み用なのでダーティーにならない", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		pageId := page.NewPageId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		_, err = bp.GetReadPage(pageId)
		assert.NoError(t, err)

		// THEN
		bufPage, err := bp.FetchPage(pageId)
		assert.NoError(t, err)
		assert.False(t, bufPage.isDirty)
	})
}

func TestFetchPage(t *testing.T) {
	t.Run("キャッシュ済みのページを取得できる", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		pageId := page.NewPageId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		bufPage, err := bp.FetchPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, pageId, bufPage.PageId)
	})

	t.Run("キャッシュにないページをディスクから読み込める", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		writePageToDisk(t, hf, 0, 0xAB)

		// WHEN
		pageId := page.NewPageId(0, 0)
		bufPage, err := bp.FetchPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(0xAB), bufPage.Page.Body[0])
	})

	t.Run("同じページを 2 回フェッチしても同じデータが返る", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		pageId := page.NewPageId(0, 0)
		addedPage, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		addedPage.Page.Body[0] = 0x42

		// WHEN
		bufPage1, err := bp.FetchPage(pageId)
		assert.NoError(t, err)
		bufPage2, err := bp.FetchPage(pageId)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, byte(0x42), bufPage1.Page.Body[0])
		assert.Equal(t, byte(0x42), bufPage2.Page.Body[0])
	})
}

func TestIsPageCached(t *testing.T) {
	t.Run("キャッシュ済みのページに対して true を返す", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		pageId := page.NewPageId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		result := bp.IsPageCached(pageId)

		// THEN
		assert.True(t, result)
	})

	t.Run("キャッシュにないページに対して false を返す", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)

		// WHEN
		result := bp.IsPageCached(page.NewPageId(0, 99))

		// THEN
		assert.False(t, result)
	})
}

func TestUnRefPage(t *testing.T) {
	t.Run("参照解除したページが優先的に追い出される", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2) // MaxNumOfPage=3
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		id0 := page.NewPageId(0, 0)
		id1 := page.NewPageId(0, 1)
		id2 := page.NewPageId(0, 2)
		_, err := bp.AddPage(id0)
		assert.NoError(t, err)
		_, err = bp.AddPage(id1)
		assert.NoError(t, err)
		_, err = bp.AddPage(id2)
		assert.NoError(t, err)

		// WHEN
		bp.UnRefPage(id0)
		newId := page.NewPageId(0, 3)
		_, err = bp.AddPage(newId)
		assert.NoError(t, err)

		// THEN
		assert.False(t, bp.IsPageCached(id0))
		assert.True(t, bp.IsPageCached(newId))
	})

	t.Run("キャッシュにないページを参照解除しても何も起きない", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		pageId := page.NewPageId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN / THEN (panic しない)
		bp.UnRefPage(page.NewPageId(0, 99))
		assert.True(t, bp.IsPageCached(pageId))
	})
}
