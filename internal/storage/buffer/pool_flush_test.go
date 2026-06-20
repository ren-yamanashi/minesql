package buffer

import (
	"sync"
	"testing"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestFlushAllPages(t *testing.T) {
	t.Run("ダーティーページがディスクに書き出されクリーンになる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.Page(pageId)
		assert.NoError(t, err)
		p.data.Body()[0] = 0xAA
		p.MarkModified()

		// WHEN
		err = bp.FlushAllPages()

		// THEN
		assert.NoError(t, err)
		bufPage, err := bp.Page(pageId)
		assert.NoError(t, err)
		assert.False(t, bufPage.isDirty)
	})

	t.Run("フラッシュ後にフラッシュリストがクリアされる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.Page(pageId)
		assert.NoError(t, err)
		p.MarkModified()

		// WHEN
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, 0, bp.FlushListPageCount())
	})

	t.Run("フラッシュ後にデータがディスクに永続化されている", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.Page(pageId)
		assert.NoError(t, err)
		p.data.Body()[0] = 0xBB
		p.MarkModified()
		err = bp.FlushAllPages()
		assert.NoError(t, err)
		bp.Unpin(pageId)

		// WHEN
		otherId := page.NewId(0, 1)
		_, err = bp.AddPage(otherId)
		assert.NoError(t, err)
		reloaded, err := bp.Page(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(0xBB), reloaded.data.Body()[0])
	})

	t.Run("ダーティーページがない場合もエラーにならない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)

		// WHEN
		err := bp.FlushAllPages()

		// THEN
		assert.NoError(t, err)
	})

	t.Run("ディスク I/O 失敗時は isDirty と flushList が更新されない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.Page(pageId)
		assert.NoError(t, err)
		p.MarkModified()
		// 強制的に HeapFile を Close して I/O を失敗させる
		_ = hf.Close()

		// WHEN
		err = bp.FlushAllPages()

		// THEN
		assert.Error(t, err)
		bufPage, perr := bp.Page(pageId)
		assert.NoError(t, perr)
		assert.True(t, bufPage.isDirty, "ディスクへ永続化されていないので isDirty が残るべき")
		assert.Equal(t, 1, bp.FlushListPageCount(), "再フラッシュ可能なように flushList に残るべき")
	})

	t.Run("X ラッチ保持中はフラッシュが完了せず、解放後に完了してクリーンになる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.Page(pageId)
		assert.NoError(t, err)
		p.MarkModified()
		mtr := NewMtr(bp)
		_, err = mtr.PageForWrite(pageId)
		assert.NoError(t, err)

		// WHEN
		done := make(chan error, 1)
		go func() {
			done <- bp.FlushAllPages()
		}()

		// THEN
		select {
		case <-done:
			t.Fatal("X ラッチ保持中はフラッシュが完了しないべき")
		case <-time.After(50 * time.Millisecond):
		}
		mtr.UnpinAll()
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("X ラッチ解放後はフラッシュが完了するべき")
		}
		bufPage, err := bp.Page(pageId)
		assert.NoError(t, err)
		assert.False(t, bufPage.isDirty)
		assert.Equal(t, 0, bp.FlushListPageCount())
	})

	t.Run("Flush と Writer が並行してもデータレースを起こさない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*4, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		var wg sync.WaitGroup
		for i := range concurrentWorkers {
			v := byte(i + 1)
			wg.Go(func() {
				for range concurrentIterations {
					mtr := NewMtr(bp)
					bufPage, err := mtr.PageForWrite(pageId)
					if err == nil {
						bufPage.data.Body()[0] = v
						bufPage.MarkModified()
					}
					mtr.UnpinAll()
				}
			})
		}
		wg.Go(func() {
			for range concurrentIterations {
				_ = bp.FlushAllPages()
			}
		})
		wg.Wait()

		// THEN
		mtr := NewMtr(bp)
		bufPage, err := mtr.PageForRead(pageId)
		assert.NoError(t, err)
		assert.Equal(t, uint64(concurrentWorkers*concurrentIterations), bufPage.modifyCount)
		mtr.UnpinAll()
	})

	t.Run("フラッシュ中の並行書き込みは次回フラッシュで永続化される (lost-update が起きない)", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*4, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		var wg sync.WaitGroup
		for i := range concurrentWorkers {
			v := byte(i + 1)
			wg.Go(func() {
				for range concurrentIterations {
					mtr := NewMtr(bp)
					bufPage, err := mtr.PageForWrite(pageId)
					if err == nil {
						bufPage.data.Body()[0] = v
						bufPage.MarkModified()
					}
					mtr.UnpinAll()
				}
			})
		}
		wg.Go(func() {
			for range concurrentIterations {
				_ = bp.FlushAllPages()
			}
		})
		wg.Wait()

		// 最終フラッシュでメモリ上の最新内容を確実にディスクへ永続化する
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// THEN
		mtr := NewMtr(bp)
		bufPage, err := mtr.PageForRead(pageId)
		assert.NoError(t, err)
		memVal := bufPage.data.Body()[0]
		mtr.UnpinAll()

		bp2 := NewPool(page.Size*2, newTestRedoLog(t), nil)
		bp2.RegisterHeapFile(0, hf)
		bufPage2, err := bp2.Page(pageId)
		assert.NoError(t, err)
		diskVal := bufPage2.data.Body()[0]
		bp2.Unpin(pageId)

		assert.Equal(t, memVal, diskVal, "メモリの最終書き込みがディスクに反映されている")
	})

	t.Run("複数ページに対する Reader/Writer/Flusher の混在ワークロードがデータレースを起こさない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*8, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageIds := []page.Id{
			page.NewId(0, 0),
			page.NewId(0, 1),
			page.NewId(0, 2),
		}
		for _, pid := range pageIds {
			_, err := bp.AddPage(pid)
			assert.NoError(t, err)
		}

		// WHEN
		var wg sync.WaitGroup
		for i := range concurrentWorkers {
			v := byte(i + 1)
			wg.Go(func() {
				for j := range concurrentIterations {
					pid := pageIds[j%len(pageIds)]
					mtr := NewMtr(bp)
					bufPage, err := mtr.PageForWrite(pid)
					if err == nil {
						bufPage.data.Body()[0] = v
						bufPage.MarkModified()
					}
					mtr.UnpinAll()
				}
			})
		}
		for range concurrentWorkers {
			wg.Go(func() {
				for j := range concurrentIterations {
					pid := pageIds[j%len(pageIds)]
					mtr := NewMtr(bp)
					bufPage, err := mtr.PageForRead(pid)
					if err == nil {
						_ = bufPage.data.Body()[0]
					}
					mtr.UnpinAll()
				}
			})
		}
		wg.Go(func() {
			for range concurrentIterations {
				_ = bp.FlushAllPages()
			}
		})
		wg.Wait()

		// THEN
		var totalModifyCount uint64
		mtr := NewMtr(bp)
		for _, pid := range pageIds {
			bufPage, err := mtr.PageForRead(pid)
			assert.NoError(t, err)
			totalModifyCount += bufPage.modifyCount
		}
		assert.Equal(t, uint64(concurrentWorkers*concurrentIterations), totalModifyCount)
		mtr.UnpinAll()
	})
}

func TestFlushOldestPages(t *testing.T) {
	t.Run("指定した件数のダーティーページをフラッシュする", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		id0 := page.NewId(0, 0)
		id1 := page.NewId(0, 1)
		_, err := bp.AddPage(id0)
		assert.NoError(t, err)
		_, err = bp.AddPage(id1)
		assert.NoError(t, err)
		p0, err := bp.Page(id0)
		assert.NoError(t, err)
		p0.MarkModified()
		p1, err := bp.Page(id1)
		assert.NoError(t, err)
		p1.MarkModified()

		// WHEN
		err = bp.FlushOldestPages(1)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, bp.FlushListPageCount())
	})

	t.Run("フラッシュリストが空の場合何もしない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)

		// WHEN
		err := bp.FlushOldestPages(10)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("フラッシュしたページがクリーンになる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.Page(pageId)
		assert.NoError(t, err)
		p.MarkModified()

		// WHEN
		err = bp.FlushOldestPages(1)
		assert.NoError(t, err)

		// THEN
		bufPage, err := bp.Page(pageId)
		assert.NoError(t, err)
		assert.False(t, bufPage.isDirty)
	})

	t.Run("ディスク I/O 失敗時は isDirty と flushList が更新されない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.Page(pageId)
		assert.NoError(t, err)
		p.MarkModified()
		// 強制的に HeapFile を Close して I/O を失敗させる
		_ = hf.Close()

		// WHEN
		err = bp.FlushOldestPages(1)

		// THEN
		assert.Error(t, err)
		bufPage, perr := bp.Page(pageId)
		assert.NoError(t, perr)
		assert.True(t, bufPage.isDirty, "ディスクへ永続化されていないので isDirty が残るべき")
		assert.Equal(t, 1, bp.FlushListPageCount(), "再フラッシュ可能なように flushList に残るべき")
	})

	t.Run("X ラッチ保持ページでブロックされても処理済みのページは順次クリーンになる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, newTestRedoLog(t), nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		id0 := page.NewId(0, 0)
		id1 := page.NewId(0, 1)
		_, err := bp.AddPage(id0)
		assert.NoError(t, err)
		_, err = bp.AddPage(id1)
		assert.NoError(t, err)
		p0, err := bp.Page(id0)
		assert.NoError(t, err)
		p0.MarkModified()
		p1, err := bp.Page(id1)
		assert.NoError(t, err)
		p1.MarkModified()
		mtr := NewMtr(bp)
		_, err = mtr.PageForWrite(id1)
		assert.NoError(t, err)

		// WHEN
		done := make(chan error, 1)
		go func() {
			done <- bp.FlushOldestPages(2)
		}()

		// THEN
		assert.Eventually(t, func() bool {
			return bp.FlushListPageCount() == 1
		}, 5*time.Second, time.Millisecond, "ブロック前に処理した id0 はクリーン化され flushList から外れるべき")
		select {
		case <-done:
			t.Fatal("X ラッチ保持中はフラッシュが完了しないべき")
		case <-time.After(50 * time.Millisecond):
		}
		mtr.UnpinAll()
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("X ラッチ解放後はフラッシュが完了するべき")
		}
		assert.False(t, bp.pages[0].isDirty)
		assert.False(t, bp.pages[1].isDirty)
		assert.Equal(t, 0, bp.FlushListPageCount())
	})
}
