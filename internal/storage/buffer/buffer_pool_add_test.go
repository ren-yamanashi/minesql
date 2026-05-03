package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestAddPage(t *testing.T) {
	t.Run("バッファプールに新しいページを追加できる", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		pageId := page.NewPageId(0, 0)

		// WHEN
		bufPage, err := bp.AddPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, bufPage)
		assert.Equal(t, pageId, bufPage.PageId)
		assert.False(t, bufPage.isDirty)
	})

	t.Run("追加したページはキャッシュされている", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize * 2)
		pageId := page.NewPageId(0, 0)

		// WHEN
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// THEN
		assert.True(t, bp.IsPageCached(pageId))
	})

	t.Run("バッファプールが満杯の場合ページを追い出して追加する", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize) // MaxNumOfPage=1
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		firstId := page.NewPageId(0, 0)
		_, err := bp.AddPage(firstId)
		assert.NoError(t, err)

		// WHEN
		secondId := page.NewPageId(0, 1)
		bufPage, err := bp.AddPage(secondId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, secondId, bufPage.PageId)
		assert.False(t, bp.IsPageCached(firstId))
	})

	t.Run("ダーティーページの追い出し時にフラッシュリストから削除される", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize) // MaxNumOfPage=1
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		firstId := page.NewPageId(0, 0)
		_, err := bp.AddPage(firstId)
		assert.NoError(t, err)
		_, err = bp.GetWritePage(firstId)
		assert.NoError(t, err)
		assert.Equal(t, uint32(1), bp.FlushListSize())

		// WHEN
		secondId := page.NewPageId(0, 1)
		_, err = bp.AddPage(secondId) // ダーティーな firstId が追い出される
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, uint32(0), bp.FlushListSize())
	})
}
