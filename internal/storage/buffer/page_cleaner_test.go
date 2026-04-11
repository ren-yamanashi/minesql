package buffer

import (
	"minesql/internal/storage/file"
	"minesql/internal/storage/log"
	"minesql/internal/storage/page"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFlushOldestPages(t *testing.T) {
	t.Run("フラッシュリストの先頭からページがフラッシュされる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := NewBufferPool(10, rl)

		disk, err := file.NewDisk(page.FileId(1), filepath.Join(tmpdir, "test.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(page.FileId(1), disk)

		pageId1, _ := bp.AllocatePageId(page.FileId(1))
		pageId2, _ := bp.AllocatePageId(page.FileId(1))
		pageId3, _ := bp.AllocatePageId(page.FileId(1))

		// 新規ページは AddPage で割り当て後、GetWritePageData で dirty 化 + FlushList 追加
		_ = bp.AddPage(pageId1)
		data1, _ := bp.GetWritePageData(pageId1)
		data1[0] = 0x11
		_ = bp.AddPage(pageId2)
		data2, _ := bp.GetWritePageData(pageId2)
		data2[0] = 0x22
		_ = bp.AddPage(pageId3)
		data3, _ := bp.GetWritePageData(pageId3)
		data3[0] = 0x33

		// WHEN
		err = bp.FlushOldestPages(2)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, 1, bp.flushList.Size)

		// フラッシュされたデータがディスクに書かれていることを確認
		reFetched, err := bp.FetchPage(pageId1)
		assert.NoError(t, err)
		assert.Equal(t, byte(0x11), reFetched.Page[0])
	})

	t.Run("フラッシュリストが空の場合は何もしない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		bp := NewBufferPool(5, nil)
		disk, _ := file.NewDisk(page.FileId(1), filepath.Join(tmpdir, "test.db"))
		bp.RegisterDisk(page.FileId(1), disk)

		// WHEN / THEN
		err := bp.FlushOldestPages(5)
		assert.NoError(t, err)
	})
}

func TestPageCleanerStartStop(t *testing.T) {
	t.Run("Start と Stop が正常に動作する", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := NewBufferPool(10, rl)

		pc := NewPageCleaner(bp, rl, 1048576, 90)
		pc.interval = 10 * time.Millisecond

		// WHEN
		pc.Start()
		time.Sleep(50 * time.Millisecond)
		pc.Stop()

		// THEN: パニックせずに正常終了する
	})
}

func TestPageCleanerFlushesOnThreshold(t *testing.T) {
	t.Run("ダーティーページ率が閾値を超えた場合にフラッシュする", func(t *testing.T) {
		// GIVEN: バッファプールサイズ 3 で 3 ページ全てダーティー (100% > 90%)
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := NewBufferPool(3, rl)
		disk, _ := file.NewDisk(page.FileId(1), filepath.Join(tmpdir, "test.db"))
		bp.RegisterDisk(page.FileId(1), disk)

		for range 3 {
			pid, _ := bp.AllocatePageId(page.FileId(1))
			_ = bp.AddPage(pid)
			data, _ := bp.GetWritePageData(pid)
			data[0] = 0x01
		}

		pc := NewPageCleaner(bp, rl, 1048576, 90)
		pc.interval = 10 * time.Millisecond

		// WHEN
		pc.Start()
		time.Sleep(100 * time.Millisecond)
		pc.Stop()

		// THEN: 一部がフラッシュされてフラッシュリストが縮小する
		assert.Less(t, bp.FlushListSize(), 3)
	})

	t.Run("REDO ログサイズが閾値を超えた場合にフラッシュする", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := NewBufferPool(10, rl)
		disk, _ := file.NewDisk(page.FileId(1), filepath.Join(tmpdir, "test.db"))
		bp.RegisterDisk(page.FileId(1), disk)

		pageId, _ := bp.AllocatePageId(page.FileId(1))
		_ = bp.AddPage(pageId)
		data, _ := bp.GetWritePageData(pageId)
		data[0] = 0x01

		// REDO ログにデータを書き込んでフラッシュし、ファイルサイズを増やす
		for range 10 {
			rl.AppendPageCopy(1, pageId, make([]byte, page.PAGE_SIZE))
		}
		err = rl.Flush()
		assert.NoError(t, err)

		pc := NewPageCleaner(bp, rl, 100, 90) // 閾値を 100 バイトに設定
		pc.interval = 10 * time.Millisecond

		// WHEN
		pc.Start()
		time.Sleep(100 * time.Millisecond)
		pc.Stop()

		// THEN: REDO ログサイズ閾値によりフラッシュされる
		assert.Equal(t, 0, bp.FlushListSize())
	})
}
