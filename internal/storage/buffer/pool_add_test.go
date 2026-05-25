package buffer

import (
	"sync/atomic"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestAddPage(t *testing.T) {
	t.Run("バッファプールに新しいページを追加できる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, nil)
		pageId := page.NewId(0, 0)

		// WHEN
		bufPage, err := bp.AddPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, bufPage)
		assert.Equal(t, pageId, bufPage.pageId)
		assert.False(t, bufPage.isDirty)
	})

	t.Run("追加したページはキャッシュされている", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, nil)
		pageId := page.NewId(0, 0)

		// WHEN
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// THEN
		_, cached := bp.pageTable.bufferId(pageId)
		assert.True(t, cached)
	})

	t.Run("AddPage は pin を増やさない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, nil)
		pageId := page.NewId(0, 0)

		// WHEN
		bufPage, err := bp.AddPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 0, bufPage.pinCount)
	})

	t.Run("バッファプールが満杯の場合ページを追い出して追加する", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size, nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		firstId := page.NewId(0, 0)
		_, err := bp.AddPage(firstId)
		assert.NoError(t, err)

		// WHEN
		secondId := page.NewId(0, 1)
		bufPage, err := bp.AddPage(secondId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, secondId, bufPage.pageId)
		_, cached := bp.pageTable.bufferId(firstId)
		assert.False(t, cached)
	})

	t.Run("Pin されたページは追い出し候補から外れ onAllPinned で解除すれば成功する", func(t *testing.T) {
		// GIVEN
		var bp *Pool
		firstId := page.NewId(0, 0)
		callCount := atomic.Int32{}
		onAllPinned := func() {
			callCount.Add(1)
			go bp.Unpin(firstId)
		}
		bp = NewPool(page.Size, onAllPinned)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		_, err := bp.AddPage(firstId)
		assert.NoError(t, err)
		_, err = bp.PageForRead(firstId)
		assert.NoError(t, err)

		// WHEN
		secondId := page.NewId(0, 1)
		_, err = bp.AddPage(secondId)

		// THEN
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, callCount.Load(), int32(1), "Pin されたページが除外されたため onAllPinned が呼ばれたはず")
		_, cached := bp.pageTable.bufferId(firstId)
		assert.False(t, cached, "Unpin 後の retry で追い出されたはず")
	})

	t.Run("ダーティーページは追い出し候補から外れ onAllPinned 経由でフラッシュすれば成功する", func(t *testing.T) {
		// GIVEN
		var bp *Pool
		firstId := page.NewId(0, 0)
		callCount := atomic.Int32{}
		onAllPinned := func() {
			callCount.Add(1)
			go func() {
				bp.Unpin(firstId)
				_ = bp.FlushAllPages()
			}()
		}
		bp = NewPool(page.Size, onAllPinned)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		_, err := bp.AddPage(firstId)
		assert.NoError(t, err)
		_, err = bp.PageForWrite(firstId)
		assert.NoError(t, err)

		// WHEN
		secondId := page.NewId(0, 1)
		_, err = bp.AddPage(secondId)

		// THEN
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, callCount.Load(), int32(1), "dirty ページが除外されたため onAllPinned が呼ばれたはず")
		_, cached := bp.pageTable.bufferId(firstId)
		assert.False(t, cached, "flush 後の retry で追い出されたはず")
	})

	t.Run("回復手段がなく追い出し候補が見つからない場合は上限到達後エラーを返す", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size, nil)
		firstId := page.NewId(0, 0)
		_, err := bp.AddPage(firstId)
		assert.NoError(t, err)
		_, err = bp.PageForRead(firstId)
		assert.NoError(t, err)

		// WHEN
		secondId := page.NewId(0, 1)
		_, err = bp.AddPage(secondId)

		// THEN
		assert.ErrorIs(t, err, ErrAllPagesUnevictable)
		_, cached := bp.pageTable.bufferId(firstId)
		assert.True(t, cached, "追い出せないので最初のページはキャッシュに残る")
	})
}
