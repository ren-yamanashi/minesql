package buffer

import (
	"encoding/binary"
	"minesql/internal/storage/file"
	"minesql/internal/storage/log"
	"minesql/internal/storage/page"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFlushAllPages(t *testing.T) {
	t.Run("ページテーブル内にダーティーページが存在する場合", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(size, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId1 := disk.AllocatePage()
		pageId2 := disk.AllocatePage()
		pageId3 := disk.AllocatePage()

		err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		err = bp.AddPage(pageId2)
		assert.NoError(t, err)
		err = bp.AddPage(pageId3)
		assert.NoError(t, err)

		// ページにデータを書き込み、ダーティーにする
		data1, _ := bp.GetWritePageData(pageId1)
		data1[0] = 11
		data2, _ := bp.GetWritePageData(pageId2)
		data2[0] = 22

		// WHEN
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// THEN
		p1, _ := bp.FetchPage(pageId1)
		p2, _ := bp.FetchPage(pageId2)
		assert.False(t, p1.IsDirty)
		assert.False(t, p2.IsDirty)

		// データがディスクに書き込まれていることを確認
		// バッファプールをクリアして、ディスクから読み直す
		bp.UnRefPage(pageId1)
		bp.UnRefPage(pageId2)
		bp.UnRefPage(pageId3)
		pageId4 := disk.AllocatePage()
		pageId5 := disk.AllocatePage()
		pageId6 := disk.AllocatePage()
		err = bp.AddPage(pageId4)
		assert.NoError(t, err)
		err = bp.AddPage(pageId5)
		assert.NoError(t, err)
		err = bp.AddPage(pageId6)
		assert.NoError(t, err)

		// page1 と page2 を再度フェッチして、データが正しく読み出せることを確認
		reFetchedPage1, err := bp.FetchPage(pageId1)
		assert.NoError(t, err)
		assert.Equal(t, byte(11), reFetchedPage1.Page[0])

		reFetchedPage2, err := bp.FetchPage(pageId2)
		assert.NoError(t, err)
		assert.Equal(t, byte(22), reFetchedPage2.Page[0])
	})

	t.Run("ページテーブル内のすべてのページがダーティーでない場合", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(size, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId1 := disk.AllocatePage()
		pageId2 := disk.AllocatePage()
		pageId3 := disk.AllocatePage()

		_ = bp.AddPage(pageId1)
		_ = bp.AddPage(pageId2)
		_ = bp.AddPage(pageId3)

		// WHEN: 全ページがクリーンな状態でフラッシュ
		err := bp.FlushAllPages()

		// THEN
		assert.NoError(t, err)
	})
}

func TestFlushAllPagesWithRedoLog(t *testing.T) {
	t.Run("REDO ログありの場合、REDO ログバッファを先にフラッシュしてからページをフラッシュする", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := NewBufferPool(5, rl)

		disk, err := file.NewDisk(page.FileId(1), filepath.Join(tmpdir, "test.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(page.FileId(1), disk)

		pageId, _ := bp.AllocatePageId(page.FileId(1))
		_ = bp.AddPage(pageId)
		data, _ := bp.GetWritePageData(pageId)
		data[0] = 0xEE

		// REDO ログバッファにレコードを追加 (未フラッシュ)
		rl.AppendPageCopy(1, pageId, data)

		// WHEN
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// THEN: REDO ログがフラッシュされている
		assert.Greater(t, rl.FlushedLSN(), log.LSN(0))
		// フラッシュリストが空になっている
		assert.Equal(t, 0, bp.FlushListSize())
	})
}

func TestFlushOldestPagesWithRedoLog(t *testing.T) {
	t.Run("REDO ログありの場合、REDO ログバッファを先にフラッシュする", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := NewBufferPool(5, rl)

		disk, err := file.NewDisk(page.FileId(1), filepath.Join(tmpdir, "test.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(page.FileId(1), disk)

		pageId, _ := bp.AllocatePageId(page.FileId(1))
		_ = bp.AddPage(pageId)
		data, _ := bp.GetWritePageData(pageId)
		data[0] = 0xDD

		// REDO ログバッファにレコードを追加 (未フラッシュ)
		rl.AppendPageCopy(1, pageId, data)

		// WHEN
		err = bp.FlushOldestPages(1)
		assert.NoError(t, err)

		// THEN: REDO ログがフラッシュされている
		assert.Greater(t, rl.FlushedLSN(), log.LSN(0))
		// フラッシュリストから除外されている
		assert.Equal(t, 0, bp.FlushListSize())
	})
}

func TestFlushOldestPagesCleanPage(t *testing.T) {
	t.Run("クリーンなページはフラッシュリストから除外されるだけでディスク書き込みしない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := NewBufferPool(10, rl)

		disk, err := file.NewDisk(page.FileId(1), filepath.Join(tmpdir, "test.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(page.FileId(1), disk)

		pageId1, _ := bp.AllocatePageId(page.FileId(1))
		_ = bp.AddPage(pageId1)
		data1, _ := bp.GetWritePageData(pageId1)
		data1[0] = 0x11

		// フラッシュリストに入っているがクリーンにする (テスト用に直接操作)
		p1, _ := bp.FetchPage(pageId1)
		p1.IsDirty = false

		// WHEN
		err = bp.FlushOldestPages(1)
		assert.NoError(t, err)

		// THEN: フラッシュリストから除外される
		assert.Equal(t, 0, bp.flushList.Size)
	})
}

func TestFlushAllPagesWithFlushList(t *testing.T) {
	t.Run("FlushAllPages 後にフラッシュリストがクリアされる", func(t *testing.T) {
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

		_ = bp.AddPage(pageId1)
		data1, _ := bp.GetWritePageData(pageId1)
		data1[0] = 0x11

		_ = bp.AddPage(pageId2)
		data2, _ := bp.GetWritePageData(pageId2)
		data2[0] = 0x22

		assert.Equal(t, 2, bp.flushList.Size)

		// WHEN
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, 0, bp.flushList.Size)
		p1, _ := bp.FetchPage(pageId1)
		p2, _ := bp.FetchPage(pageId2)
		assert.False(t, p1.IsDirty)
		assert.False(t, p2.IsDirty)
	})
}

func TestPopNewlyDirtied(t *testing.T) {
	t.Run("GetWritePageData で新しくダーティーになったページが返される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(5, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId1 := disk.AllocatePage()
		pageId2 := disk.AllocatePage()

		err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		err = bp.AddPage(pageId2)
		assert.NoError(t, err)

		// WHEN: 2 ページを GetWritePageData でダーティーにする
		_, err = bp.GetWritePageData(pageId1)
		assert.NoError(t, err)
		_, err = bp.GetWritePageData(pageId2)
		assert.NoError(t, err)

		newlyDirtied := bp.PopNewlyDirtied()

		// THEN
		assert.Equal(t, 2, len(newlyDirtied))
		assert.Contains(t, newlyDirtied, pageId1)
		assert.Contains(t, newlyDirtied, pageId2)
	})

	t.Run("PopNewlyDirtied 後は空になる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(5, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId := disk.AllocatePage()
		err := bp.AddPage(pageId)
		assert.NoError(t, err)
		_, err = bp.GetWritePageData(pageId)
		assert.NoError(t, err)

		bp.PopNewlyDirtied()

		// WHEN
		secondDrain := bp.PopNewlyDirtied()

		// THEN
		assert.Empty(t, secondDrain)
	})

	t.Run("既にダーティーなページを再度 GetWritePageData しても追跡される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(5, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId := disk.AllocatePage()
		err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// 1 回目の GetWritePageData
		_, err = bp.GetWritePageData(pageId)
		assert.NoError(t, err)
		bp.PopNewlyDirtied()

		// WHEN: 2 回目の GetWritePageData (既にダーティー)
		_, err = bp.GetWritePageData(pageId)
		assert.NoError(t, err)
		result := bp.PopNewlyDirtied()

		// THEN: 既にダーティーでも書き込みごとに追跡される (REDO ログ記録のため)
		assert.Equal(t, 1, len(result))
		assert.Equal(t, pageId, result[0])
	})
}

func TestClearNewlyDirtied(t *testing.T) {
	t.Run("ClearNewlyDirtied 後は PopNewlyDirtied が空を返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(5, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId := disk.AllocatePage()
		err := bp.AddPage(pageId)
		assert.NoError(t, err)
		_, err = bp.GetWritePageData(pageId)
		assert.NoError(t, err)

		// WHEN
		bp.ClearNewlyDirtied()

		// THEN
		result := bp.PopNewlyDirtied()
		assert.Empty(t, result)
	})
}

func TestMinPageLSN(t *testing.T) {
	t.Run("ダーティーページの最小 Page LSN を返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(5, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId1 := disk.AllocatePage()
		pageId2 := disk.AllocatePage()
		pageId3 := disk.AllocatePage()

		_ = bp.AddPage(pageId1)
		_ = bp.AddPage(pageId2)
		_ = bp.AddPage(pageId3)

		// 各ページをダーティーにし、Page LSN を設定
		data1, _ := bp.GetWritePageData(pageId1)
		binary.BigEndian.PutUint32(page.NewPage(data1).Header, 10)

		data2, _ := bp.GetWritePageData(pageId2)
		binary.BigEndian.PutUint32(page.NewPage(data2).Header, 5)

		data3, _ := bp.GetWritePageData(pageId3)
		binary.BigEndian.PutUint32(page.NewPage(data3).Header, 15)

		// WHEN
		minLSN := bp.MinPageLSN()

		// THEN
		assert.Equal(t, uint32(5), minLSN)
	})

	t.Run("ダーティーページがない場合は 0 を返す", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(5, nil)

		// WHEN
		minLSN := bp.MinPageLSN()

		// THEN
		assert.Equal(t, uint32(0), minLSN)
	})

	t.Run("ダーティーページが 1 つだけの場合はその Page LSN を返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(5, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId := disk.AllocatePage()
		_ = bp.AddPage(pageId)
		data, _ := bp.GetWritePageData(pageId)
		binary.BigEndian.PutUint32(page.NewPage(data).Header, 42)

		// WHEN
		minLSN := bp.MinPageLSN()

		// THEN
		assert.Equal(t, uint32(42), minLSN)
	})
}
