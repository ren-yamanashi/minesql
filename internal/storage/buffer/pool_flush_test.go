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
		p.data.Body()[0] = 0xAA

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
		bp := NewPool(page.Size)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.PageForWrite(pageId)
		assert.NoError(t, err)
		p.data.Body()[0] = 0xBB
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// WHEN
		otherId := page.NewId(0, 1)
		_, err = bp.AddPage(otherId)
		assert.NoError(t, err)
		reloaded, err := bp.PageForRead(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(0xBB), reloaded.data.Body()[0])
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

	t.Run("ディスク I/O 失敗時は isDirty と flushList が更新されない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 2)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		_, err = bp.PageForWrite(pageId)
		assert.NoError(t, err)
		// 強制的に HeapFile を Close して I/O を失敗させる
		_ = hf.Close()

		// WHEN
		err = bp.FlushAllPages()

		// THEN
		assert.Error(t, err)
		bufPage, perr := bp.PageForRead(pageId)
		assert.NoError(t, perr)
		assert.True(t, bufPage.isDirty, "ディスクへ永続化されていないので isDirty が残るべき")
		assert.Equal(t, 1, bp.FlushListPageCount(), "再フラッシュ可能なように flushList に残るべき")
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

	t.Run("ディスク I/O 失敗時は isDirty と flushList が更新されない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size * 2)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		_, err = bp.PageForWrite(pageId)
		assert.NoError(t, err)
		// 強制的に HeapFile を Close して I/O を失敗させる
		_ = hf.Close()

		// WHEN
		err = bp.FlushOldestPages(1)

		// THEN
		assert.Error(t, err)
		bufPage, perr := bp.PageForRead(pageId)
		assert.NoError(t, perr)
		assert.True(t, bufPage.isDirty, "ディスクへ永続化されていないので isDirty が残るべき")
		assert.Equal(t, 1, bp.FlushListPageCount(), "再フラッシュ可能なように flushList に残るべき")
	})
}
