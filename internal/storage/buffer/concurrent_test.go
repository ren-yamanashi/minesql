package buffer

import (
	"sync"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

const (
	concurrentWorkers    = 4
	concurrentIterations = 50
)

func TestConcurrentWritersDoNotRace(t *testing.T) {
	t.Run("複数の Mtr が同一ページへ並行に書き込んでもデータレースを起こさない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*4, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		var wg sync.WaitGroup
		for i := range concurrentWorkers {
			wg.Add(1)
			go func(v byte) {
				defer wg.Done()
				for range concurrentIterations {
					mtr := NewMtr(bp)
					bufPage, err := mtr.PageForWrite(pageId)
					if err == nil {
						bufPage.data.Body()[0] = v
					}
					mtr.UnpinAll()
				}
			}(byte(i + 1))
		}
		wg.Wait()

		// THEN
		mtr := NewMtr(bp)
		bufPage, err := mtr.PageForRead(pageId)
		assert.NoError(t, err)
		assert.NotEqual(t, byte(0), bufPage.data.Body()[0])
		mtr.UnpinAll()
	})
}

func TestConcurrentReadersAndWriterDoNotRace(t *testing.T) {
	t.Run("並行する Reader と Writer がデータレースを起こさない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*4, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		var wg sync.WaitGroup
		// Writer
		for i := range concurrentWorkers {
			wg.Add(1)
			go func(v byte) {
				defer wg.Done()
				for range concurrentIterations {
					mtr := NewMtr(bp)
					bufPage, err := mtr.PageForWrite(pageId)
					if err == nil {
						bufPage.data.Body()[0] = v
					}
					mtr.UnpinAll()
				}
			}(byte(i + 1))
		}
		// Reader
		for range concurrentWorkers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range concurrentIterations {
					mtr := NewMtr(bp)
					bufPage, err := mtr.PageForRead(pageId)
					if err == nil {
						_ = bufPage.data.Body()[0]
					}
					mtr.UnpinAll()
				}
			}()
		}
		wg.Wait()

		// THEN
		mtr := NewMtr(bp)
		bufPage, err := mtr.PageForRead(pageId)
		assert.NoError(t, err)
		assert.Equal(t, uint64(concurrentWorkers*concurrentIterations), bufPage.modifyCount)
		mtr.UnpinAll()
	})
}

func TestConcurrentWriterAndFlusherDoNotRace(t *testing.T) {
	t.Run("Flush と Writer が並行してもデータレースを起こさない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*4, nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		var wg sync.WaitGroup
		// Writer
		for i := range concurrentWorkers {
			wg.Add(1)
			go func(v byte) {
				defer wg.Done()
				for range concurrentIterations {
					mtr := NewMtr(bp)
					bufPage, err := mtr.PageForWrite(pageId)
					if err == nil {
						bufPage.data.Body()[0] = v
					}
					mtr.UnpinAll()
				}
			}(byte(i + 1))
		}
		// Flusher
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range concurrentIterations {
				_ = bp.FlushAllPages()
			}
		}()
		wg.Wait()

		// THEN
		mtr := NewMtr(bp)
		bufPage, err := mtr.PageForRead(pageId)
		assert.NoError(t, err)
		assert.Equal(t, uint64(concurrentWorkers*concurrentIterations), bufPage.modifyCount)
		mtr.UnpinAll()
	})
}

func TestConcurrentMixedWorkloadOnMultiplePages(t *testing.T) {
	t.Run("複数ページに対する Reader/Writer/Flusher の混在ワークロードがデータレースを起こさない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*8, nil)
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
		// Writer
		for i := range concurrentWorkers {
			wg.Add(1)
			go func(v byte) {
				defer wg.Done()
				for j := range concurrentIterations {
					pid := pageIds[j%len(pageIds)]
					mtr := NewMtr(bp)
					bufPage, err := mtr.PageForWrite(pid)
					if err == nil {
						bufPage.data.Body()[0] = v
					}
					mtr.UnpinAll()
				}
			}(byte(i + 1))
		}
		// Reader
		for range concurrentWorkers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range concurrentIterations {
					pid := pageIds[j%len(pageIds)]
					mtr := NewMtr(bp)
					bufPage, err := mtr.PageForRead(pid)
					if err == nil {
						_ = bufPage.data.Body()[0]
					}
					mtr.UnpinAll()
				}
			}()
		}
		// Flusher
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range concurrentIterations {
				_ = bp.FlushAllPages()
			}
		}()
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
