package buffer

import (
	"minesql/internal/storage/file"
	"minesql/internal/storage/log"
	"minesql/internal/storage/page"
	"path/filepath"
	"testing"

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

		p1, _ := bp.AddPage(pageId1)
		p1.GetWriteData()[0] = 0x11
		bp.FlushList.Add(pageId1)

		p2, _ := bp.AddPage(pageId2)
		p2.GetWriteData()[0] = 0x22
		bp.FlushList.Add(pageId2)

		p3, _ := bp.AddPage(pageId3)
		p3.GetWriteData()[0] = 0x33
		bp.FlushList.Add(pageId3)

		// WHEN
		err = bp.FlushOldestPages(2)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, 1, bp.FlushList.Size)
		assert.False(t, p1.IsDirty)
		assert.False(t, p2.IsDirty)
		assert.True(t, p3.IsDirty)

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

func TestFlushIfNeeded(t *testing.T) {
	t.Run("閾値を超えていない場合はフラッシュしない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := NewBufferPool(100, rl)
		disk, _ := file.NewDisk(page.FileId(1), filepath.Join(tmpdir, "test.db"))
		bp.RegisterDisk(page.FileId(1), disk)

		pageId, _ := bp.AllocatePageId(page.FileId(1))
		p, _ := bp.AddPage(pageId)
		p.GetWriteData()[0] = 0x01
		bp.FlushList.Add(pageId)

		pc := NewPageCleaner(bp, rl, 1048576, 90) // 1MB, 90%

		// WHEN
		err = pc.FlushIfNeeded()
		assert.NoError(t, err)

		// THEN: 1/100 = 1% なのでフラッシュされない
		assert.Equal(t, 1, bp.FlushList.Size)
		assert.True(t, p.IsDirty)
	})

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
			p, _ := bp.AddPage(pid)
			p.GetWriteData()[0] = 0x01
			bp.FlushList.Add(pid)
		}

		pc := NewPageCleaner(bp, rl, 1048576, 90)

		// WHEN
		err = pc.FlushIfNeeded()
		assert.NoError(t, err)

		// THEN: 一部がフラッシュされてフラッシュリストが縮小する
		assert.Less(t, bp.FlushList.Size, 3)
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
		p, _ := bp.AddPage(pageId)
		p.GetWriteData()[0] = 0x01
		bp.FlushList.Add(pageId)

		// REDO ログにデータを書き込んでフラッシュし、ファイルサイズを増やす
		for range 10 {
			rl.AppendPageCopy(1, pageId, make([]byte, page.PAGE_SIZE))
		}
		err = rl.Flush()
		assert.NoError(t, err)

		pc := NewPageCleaner(bp, rl, 100, 90) // 閾値を 100 バイトに設定

		// WHEN
		err = pc.FlushIfNeeded()
		assert.NoError(t, err)

		// THEN: REDO ログサイズ閾値により 1 ページがフラッシュされる
		assert.Equal(t, 0, bp.FlushList.Size)
	})
}
