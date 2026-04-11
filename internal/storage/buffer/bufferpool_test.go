package buffer

import (
	"minesql/internal/storage/file"
	"minesql/internal/storage/log"
	"minesql/internal/storage/page"
	"path/filepath"
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/assert"
)

func TestNewBufferPool(t *testing.T) {
	t.Run("正常にバッファプールマネージャが生成される", func(t *testing.T) {
		// GIVEN
		size := 5
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		fileId := page.FileId(0)
		disk, err := file.NewDisk(fileId, path)
		assert.NoError(t, err)

		// WHEN
		bp := NewBufferPool(size)
		bp.RegisterDisk(fileId, disk)

		// THEN
		assert.NotNil(t, bp)
		registeredDm, err := bp.GetDisk(fileId)
		assert.NoError(t, err)
		assert.Equal(t, disk, registeredDm)
		assert.Equal(t, size, bp.maxBufferSize)
		assert.Equal(t, size, len(bp.bufferPages))
		assert.Equal(t, 0, len(bp.pageTable))
	})
}

func TestRegisterDisk(t *testing.T) {
	t.Run("Disk が正しく登録される", func(t *testing.T) {
		// GIVEN
		size := 5
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		fileId := page.FileId(0)
		disk, err := file.NewDisk(fileId, path)
		assert.NoError(t, err)

		bp := NewBufferPool(size)

		// WHEN
		bp.RegisterDisk(fileId, disk)

		// THEN
		retrievedDm, err := bp.GetDisk(fileId)
		assert.NoError(t, err)
		assert.Equal(t, disk, retrievedDm)
	})
}

func TestGetDisk(t *testing.T) {
	t.Run("登録されている Disk を取得できる", func(t *testing.T) {
		// GIVEN
		size := 5
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		fileId := page.FileId(0)
		disk, err := file.NewDisk(fileId, path)
		assert.NoError(t, err)

		bp := NewBufferPool(size)
		bp.RegisterDisk(fileId, disk)

		// WHEN
		retrievedDm, err := bp.GetDisk(fileId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, disk, retrievedDm)
	})

	t.Run("登録されていない FileId を指定するとエラーが発生する", func(t *testing.T) {
		// GIVEN
		size := 5
		bp := NewBufferPool(size)

		// WHEN
		nonExistentFileId := page.FileId(999)
		retrievedDm, err := bp.GetDisk(nonExistentFileId)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, retrievedDm)
		assert.Contains(t, err.Error(), "disk for FileId 999 not found")
	})
}

func TestAllocatePageId(t *testing.T) {
	t.Run("登録された FileId に対して PageId が正しく割り当てられる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		bp := NewBufferPool(10)
		fileId := page.FileId(1)
		path := filepath.Join(tmpdir, "test.db")
		disk, err := file.NewDisk(fileId, path)
		assert.NoError(t, err)
		bp.RegisterDisk(fileId, disk)

		// WHEN
		pageId1, err := bp.AllocatePageId(fileId)
		assert.NoError(t, err)
		pageId2, err := bp.AllocatePageId(fileId)
		assert.NoError(t, err)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, fileId, pageId1.FileId)
		assert.Equal(t, fileId, pageId2.FileId)
		assert.Equal(t, page.PageNumber(0), pageId1.PageNumber)
		assert.Equal(t, page.PageNumber(1), pageId2.PageNumber)
	})

	t.Run("登録されていない FileId に対してエラーが返される", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(10)
		nonExistentFileId := page.FileId(999)

		// WHEN
		pageId, err := bp.AllocatePageId(nonExistentFileId)

		// THEN
		assert.Error(t, err)
		assert.Equal(t, page.INVALID_PAGE_ID, pageId)
		assert.Contains(t, err.Error(), "disk for FileId 999 not found")
	})

	t.Run("複数の FileId に対してそれぞれ独立した PageId が割り当てられる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		bp := NewBufferPool(10)

		fileId1 := page.FileId(1)
		path1 := filepath.Join(tmpdir, "test1.db")
		dm1, err := file.NewDisk(fileId1, path1)
		assert.NoError(t, err)
		bp.RegisterDisk(fileId1, dm1)

		fileId2 := page.FileId(2)
		path2 := filepath.Join(tmpdir, "test2.db")
		dm2, err := file.NewDisk(fileId2, path2)
		assert.NoError(t, err)
		bp.RegisterDisk(fileId2, dm2)

		// WHEN
		pageId1_1, err := bp.AllocatePageId(fileId1)
		assert.NoError(t, err)
		pageId2_1, err := bp.AllocatePageId(fileId2)
		assert.NoError(t, err)
		pageId1_2, err := bp.AllocatePageId(fileId1)
		assert.NoError(t, err)
		pageId2_2, err := bp.AllocatePageId(fileId2)
		assert.NoError(t, err)

		// THEN
		// FileId1 のページ
		assert.Equal(t, fileId1, pageId1_1.FileId)
		assert.Equal(t, fileId1, pageId1_2.FileId)
		assert.Equal(t, page.PageNumber(0), pageId1_1.PageNumber)
		assert.Equal(t, page.PageNumber(1), pageId1_2.PageNumber)

		// FileId2 のページ
		assert.Equal(t, fileId2, pageId2_1.FileId)
		assert.Equal(t, fileId2, pageId2_2.FileId)
		assert.Equal(t, page.PageNumber(0), pageId2_1.PageNumber)
		assert.Equal(t, page.PageNumber(1), pageId2_2.PageNumber)
	})
}

func TestFetchPage(t *testing.T) {
	t.Run("指定されたページがページテーブルに存在する場合、同じページが返される", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		disk, pageId := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(size)
		bp.RegisterDisk(page.FileId(0), disk)

		bufferPage, err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		fetchedPage, err := bp.FetchPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, bufferPage, fetchedPage)
		assert.Equal(t, BufferId(2), bp.pageTable[pageId])
	})

	t.Run("指定されたページがページテーブルに存在しない場合、ディスクからページが読み込まれる", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		disk, pageId := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(size)
		bp.RegisterDisk(page.FileId(0), disk)

		// WHEN
		fetchedPage, err := bp.FetchPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, fetchedPage)
		assert.Equal(t, pageId, fetchedPage.PageId)
		assert.False(t, fetchedPage.IsDirty)
		assert.Equal(t, BufferId(2), bp.pageTable[pageId])
	})
}

func TestAddPage(t *testing.T) {
	t.Run("バッファプールに空きがある場合、新しいページが追加される", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(size)
		bp.RegisterDisk(page.FileId(0), disk)
		pageId := disk.AllocatePage()

		// WHEN
		bufferPage, err := bp.AddPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, bufferPage)
		assert.Equal(t, pageId, bufferPage.PageId)
		bufferId, ok := bp.pageTable[pageId]
		assert.True(t, ok)
		assert.Equal(t, BufferId(2), bufferId)
	})

	t.Run("バッファプールに空きがない場合、かつ該当のページがダーティーな場合、一度ページの内容をディスクに書き込んだ後、ページが置換される", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(size)
		bp.RegisterDisk(page.FileId(0), disk)

		// バッファプールを満杯にする
		pageId1 := disk.AllocatePage()
		pageId2 := disk.AllocatePage()
		pageId3 := disk.AllocatePage()

		page1, err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId2)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId3)
		assert.NoError(t, err)

		// page1 にデータを書き込み、ダーティーにする
		page1.Page[0] = 99
		page1.IsDirty = true

		// pageId1 を最後に参照解除して、LRU の末尾に配置し最初に追い出されるようにする
		bp.UnRefPage(pageId2)
		bp.UnRefPage(pageId3)
		bp.UnRefPage(pageId1)

		// WHEN
		pageId4 := disk.AllocatePage()
		newPage, err := bp.AddPage(pageId4)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, newPage)
		assert.Equal(t, pageId4, newPage.PageId)
		// 新しいページがページテーブルに追加されていることを確認
		_, ok := bp.pageTable[pageId4]
		assert.True(t, ok)
		// 古いページ (pageId1) がページテーブルから削除されていることを確認
		_, ok = bp.pageTable[pageId1]
		assert.False(t, ok)

		// page1 のデータがディスクに書き込まれていることを確認
		// page1 を再度フェッチして、データが正しく読み出せることを確認
		reFetchedPage1, err := bp.FetchPage(pageId1)
		assert.NoError(t, err)
		assert.Equal(t, byte(99), reFetchedPage1.Page[0])
	})

	t.Run("バッファプールに空きがない場合、かつ該当のページがダーティーでない場合、そのままページが置換される", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(size)
		bp.RegisterDisk(page.FileId(0), disk)

		// バッファプールを満杯にする
		pageId1 := disk.AllocatePage()
		pageId2 := disk.AllocatePage()
		pageId3 := disk.AllocatePage()

		_, err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId2)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId3)
		assert.NoError(t, err)

		// pageId1 を最後に参照解除して、LRU の末尾に配置し最初に追い出されるようにする
		// IsDirty を false に設定
		bp.UnRefPage(pageId2)
		bp.UnRefPage(pageId3)
		bp.UnRefPage(pageId1)
		bp.bufferPages[0].IsDirty = false
		bp.bufferPages[1].IsDirty = false
		bp.bufferPages[2].IsDirty = false

		// WHEN
		pageId4 := disk.AllocatePage()
		newPage, err := bp.AddPage(pageId4)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, newPage)
		assert.Equal(t, pageId4, newPage.PageId)
		// 新しいページがページテーブルに追加されていることを確認
		_, ok := bp.pageTable[pageId4]
		assert.True(t, ok)
		// 古いページ (pageId1) がページテーブルから削除されていることを確認
		_, ok = bp.pageTable[pageId1]
		assert.False(t, ok)
	})
}

func TestUnRefPage(t *testing.T) {
	t.Run("UnRefPage したページが優先的に追い出される", func(t *testing.T) {
		// GIVEN: 3 ページを読み込み、page2 のみ参照を解除
		size := 3
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(size)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId1 := disk.AllocatePage()
		pageId2 := disk.AllocatePage()
		pageId3 := disk.AllocatePage()

		_, err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId2)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId3)
		assert.NoError(t, err)

		// page2 のみ参照を解除
		bp.UnRefPage(pageId2)

		// WHEN: 新しいページを追加 (追い出しが発生)
		pageId4 := disk.AllocatePage()
		_, err = bp.AddPage(pageId4)
		assert.NoError(t, err)

		// THEN: page2 が追い出され、page1, page3, page4 がバッファに残る
		_, ok := bp.pageTable[pageId2]
		assert.False(t, ok)
		_, ok = bp.pageTable[pageId1]
		assert.True(t, ok)
		_, ok = bp.pageTable[pageId3]
		assert.True(t, ok)
		_, ok = bp.pageTable[pageId4]
		assert.True(t, ok)
	})
}

func TestFlushPage(t *testing.T) {
	t.Run("ページテーブル内にダーティーページが存在する場合", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(size)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId1 := disk.AllocatePage()
		pageId2 := disk.AllocatePage()
		pageId3 := disk.AllocatePage()

		page1, err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		page2, err := bp.AddPage(pageId2)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId3)
		assert.NoError(t, err)

		// ページにデータを書き込み、ダーティーにする
		page1.Page[0] = 11
		page1.IsDirty = true
		page2.Page[0] = 22
		page2.IsDirty = true

		// WHEN
		err = bp.FlushPage()
		assert.NoError(t, err)

		// THEN
		assert.NoError(t, err)
		assert.False(t, page1.IsDirty)
		assert.False(t, page2.IsDirty)

		// データがディスクに書き込まれていることを確認
		// バッファプールをクリアして、ディスクから読み直す
		bp.UnRefPage(pageId1)
		bp.UnRefPage(pageId2)
		bp.UnRefPage(pageId3)
		pageId4 := disk.AllocatePage()
		pageId5 := disk.AllocatePage()
		pageId6 := disk.AllocatePage()
		_, err = bp.AddPage(pageId4)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId5)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId6)
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
		bp := NewBufferPool(size)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId1 := disk.AllocatePage()
		pageId2 := disk.AllocatePage()
		pageId3 := disk.AllocatePage()

		page1, _ := bp.AddPage(pageId1)
		page2, _ := bp.AddPage(pageId2)
		page3, _ := bp.AddPage(pageId3)

		page1.IsDirty = false
		page2.IsDirty = false
		page3.IsDirty = false

		// WHEN
		err := bp.FlushPage()

		// THEN
		assert.NoError(t, err)
		assert.False(t, page1.IsDirty)
		assert.False(t, page2.IsDirty)
		assert.False(t, page3.IsDirty)
	})
}

func TestSetRedoLog(t *testing.T) {
	t.Run("RedoLog が設定される", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(3)
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)

		// WHEN
		bp.SetRedoLog(rl)

		// THEN
		assert.Equal(t, rl, bp.redoLog)
	})
}

func TestDirtyPageIds(t *testing.T) {
	t.Run("ダーティーページがない場合は空のリストを返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(3)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId := disk.AllocatePage()
		p, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p.IsDirty = false

		// WHEN
		dirtyPages := bp.DirtyPageIds()

		// THEN
		assert.Empty(t, dirtyPages)
	})

	t.Run("ダーティーページの PageId リストを返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(5)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId1 := disk.AllocatePage()
		pageId2 := disk.AllocatePage()
		pageId3 := disk.AllocatePage()

		p1, err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		p2, err := bp.AddPage(pageId2)
		assert.NoError(t, err)
		p3, err := bp.AddPage(pageId3)
		assert.NoError(t, err)

		// pageId1 と pageId3 のみダーティーにする
		p1.IsDirty = true
		p2.IsDirty = false
		p3.IsDirty = true

		// WHEN
		dirtyPages := bp.DirtyPageIds()

		// THEN
		assert.Equal(t, 2, len(dirtyPages))
		assert.Contains(t, dirtyPages, pageId1)
		assert.Contains(t, dirtyPages, pageId3)
	})
}

func TestBufferPoolIntegration(t *testing.T) {
	t.Run("バッファプールの統合動作テスト (ページアクセス、ページ置換)", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		fileId := page.FileId(0)
		disk, err := file.NewDisk(fileId, path)
		assert.NoError(t, err)
		bp := NewBufferPool(3)
		bp.RegisterDisk(fileId, disk)

		// ページを作成
		page1 := disk.AllocatePage()
		page2 := disk.AllocatePage()
		page3 := disk.AllocatePage()
		page4 := disk.AllocatePage()
		page5 := disk.AllocatePage()

		// 各ページにデータを書き込む (PageID と同じ値を書き込む)
		writeTestData := func(pageId page.PageId, value byte) {
			data := directio.AlignedBlock(directio.BlockSize)
			for i := range data {
				data[i] = value
			}
			err := disk.WritePageData(pageId, data)
			assert.NoError(t, err)
		}

		writeTestData(page1, byte(page1.PageNumber))
		writeTestData(page2, byte(page2.PageNumber))
		writeTestData(page3, byte(page3.PageNumber))
		writeTestData(page4, byte(page4.PageNumber))
		writeTestData(page5, byte(page5.PageNumber))

		// ---------------------------------------
		// ページアクセスのシミュレーション
		// ---------------------------------------

		// ### 1. page1, page2, page3 をフェッチ (バッファプールに読み込まれる)
		fetchedPage1, err := bp.FetchPage(page1)
		assert.NoError(t, err)
		assert.Equal(t, byte(page1.PageNumber), fetchedPage1.Page[0])

		fetchedPage2, err := bp.FetchPage(page2)
		assert.NoError(t, err)
		assert.Equal(t, byte(page2.PageNumber), fetchedPage2.Page[0])

		fetchedPage3, err := bp.FetchPage(page3)
		assert.NoError(t, err)
		assert.Equal(t, byte(page3.PageNumber), fetchedPage3.Page[0])

		assert.Equal(t, 3, len(bp.pageTable)) // バッファプールが満杯になっている

		// ### 2. page4 をアクセス (ページ置換発生)
		// LRU の末尾にある page1 が追い出される
		fetchedPage4, err := bp.FetchPage(page4)
		assert.NoError(t, err)
		assert.Equal(t, byte(page4.PageNumber), fetchedPage4.Page[0])

		// page1 がページテーブルから削除される
		_, page1InBuffer := bp.pageTable[page1]
		assert.False(t, page1InBuffer)

		// page4 がバッファプールに追加される
		_, ok := bp.pageTable[page4]
		assert.True(t, ok)

		// ### 3. page5 をアクセス (ページ置換発生)
		// LRU の末尾にある page2 が追い出される
		fetchedPage5, err := bp.FetchPage(page5)
		assert.NoError(t, err)
		assert.Equal(t, byte(page5.PageNumber), fetchedPage5.Page[0])

		// page2 がページテーブルから削除されることを確認
		_, page2InBuffer := bp.pageTable[page2]
		assert.False(t, page2InBuffer)

		// page5 がバッファプールに追加される
		_, ok = bp.pageTable[page5]
		assert.True(t, ok)

		// ### 4. page1 を再度アクセス
		// page1 がバッファから追い出されているため、再度ディスクから読み込まれる
		// page3 が追い出される
		reFetchedPage1, err := bp.FetchPage(page1)
		assert.NoError(t, err)
		assert.Equal(t, byte(page1.PageNumber), reFetchedPage1.Page[0])

		// page3 がページテーブルから削除されることを確認
		_, page3InBuffer := bp.pageTable[page3]
		assert.False(t, page3InBuffer)

		// page1 がバッファプールに存在する
		_, page1InBuffer = bp.pageTable[page1]
		assert.True(t, page1InBuffer)

		// 最終的に page4, page5, page1 がバッファプールに存在
		assert.Equal(t, 3, len(bp.pageTable))
		assert.Contains(t, bp.pageTable, page4)
		assert.Contains(t, bp.pageTable, page5)
		assert.Contains(t, bp.pageTable, page1)
	})
}

func createEmptyDisk(t *testing.T, tmpdir string) (*file.Disk, page.PageId) {
	path := filepath.Join(tmpdir, "test.db")
	disk, err := file.NewDisk(page.FileId(0), path)
	assert.NoError(t, err)
	pageId := disk.AllocatePage()

	// ページをディスクに書き込む (空のページ)
	emptyPage := directio.AlignedBlock(directio.BlockSize)
	err = disk.WritePageData(pageId, emptyPage)
	assert.NoError(t, err)

	return disk, pageId
}
