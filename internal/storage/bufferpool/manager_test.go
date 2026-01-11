package bufferpool

import (
	"minesql/internal/storage/disk"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBufferPoolManager(t *testing.T) {
	t.Run("正常にバッファプールマネージャが生成される", func(t *testing.T) {
		// GIVEN
		size := 5
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		fileId := disk.FileId(0)
		dm, err := disk.NewDiskManager(fileId, path)
		assert.NoError(t, err)

		// WHEN
		bpm := NewBufferPoolManager(size, tmpdir)
		bpm.RegisterDiskManager(fileId, dm)

		// THEN
		assert.NotNil(t, bpm)
		registeredDm, err := bpm.GetDiskManager(fileId)
		assert.NoError(t, err)
		assert.Equal(t, dm, registeredDm)
		assert.Equal(t, size, bpm.bufpool.MaxBufferSize)
		assert.Equal(t, size, len(bpm.bufpool.BufferPages))
		assert.Equal(t, 0, len(bpm.pageTable))
	})
}

func TestFetchPage(t *testing.T) {
	t.Run("指定されたページがページテーブルに存在する場合、同じページが返される", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		dm, pageId := initDiskManager(t, tmpdir)
		bpm := NewBufferPoolManager(size, tmpdir)
		bpm.RegisterDiskManager(disk.FileId(0), dm)

		bufferPage, err := bpm.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		fetchedPage, err := bpm.FetchPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, bufferPage, fetchedPage)
		assert.Equal(t, bpm.pageTable[pageId], BufferId(0))
		assert.True(t, fetchedPage.Referenced)
	})

	t.Run("指定されたページがページテーブルに存在しない場合、ディスクからページが読み込まれる", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		dm, pageId := initDiskManager(t, tmpdir)
		bpm := NewBufferPoolManager(size, tmpdir)
		bpm.RegisterDiskManager(disk.FileId(0), dm)

		// WHEN
		fetchedPage, err := bpm.FetchPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, fetchedPage)
		assert.Equal(t, pageId, fetchedPage.PageId)
		assert.True(t, fetchedPage.Referenced)
		assert.False(t, fetchedPage.IsDirty)
		assert.Equal(t, BufferId(0), bpm.pageTable[pageId])
	})
}

func TestAddPage(t *testing.T) {
	t.Run("バッファプールに空きがある場合、新しいページが追加される", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		dm, _ := initDiskManager(t, tmpdir)
		bpm := NewBufferPoolManager(size, tmpdir)
		bpm.RegisterDiskManager(disk.FileId(0), dm)
		pageId := dm.AllocatePage()

		// WHEN
		bufferPage, err := bpm.AddPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, bufferPage)
		assert.Equal(t, pageId, bufferPage.PageId)
		bufferId, ok := bpm.pageTable[pageId]
		assert.True(t, ok)
		assert.Equal(t, BufferId(0), bufferId)
	})

	t.Run("バッファプールに空きがない場合、かつ該当のページがダーティーな場合、一度ページの内容をディスクに書き込んだ後、ページが置換される", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		dm, _ := initDiskManager(t, tmpdir)
		bpm := NewBufferPoolManager(size, tmpdir)
		bpm.RegisterDiskManager(disk.FileId(0), dm)

		// バッファプールを満杯にする
		pageId1 := dm.AllocatePage()
		pageId2 := dm.AllocatePage()
		pageId3 := dm.AllocatePage()

		page1, _ := bpm.AddPage(pageId1)
		bpm.AddPage(pageId2)
		bpm.AddPage(pageId3)

		// page1 にデータを書き込み、ダーティーにする
		page1.Page[0] = 99
		page1.IsDirty = true

		// すべてのページの Referenced を false にして、最初のページが選ばれるようにする
		bpm.bufpool.BufferPages[0].Referenced = false
		bpm.bufpool.BufferPages[1].Referenced = false
		bpm.bufpool.BufferPages[2].Referenced = false

		// WHEN
		pageId4 := dm.AllocatePage()
		newPage, err := bpm.AddPage(pageId4)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, newPage)
		assert.Equal(t, pageId4, newPage.PageId)
		// 新しいページがページテーブルに追加されていることを確認
		_, ok := bpm.pageTable[pageId4]
		assert.True(t, ok)
		// 古いページ (pageId1) がページテーブルから削除されていることを確認
		_, ok = bpm.pageTable[pageId1]
		assert.False(t, ok)

		// page1 のデータがディスクに書き込まれていることを確認
		// page1 を再度フェッチして、データが正しく読み出せることを確認
		reFetchedPage1, err := bpm.FetchPage(pageId1)
		assert.NoError(t, err)
		assert.Equal(t, byte(99), reFetchedPage1.Page[0])
	})

	t.Run("バッファプールに空きがない場合、かつ該当のページがダーティーでない場合、そのままページが置換される", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		dm, _ := initDiskManager(t, tmpdir)
		bpm := NewBufferPoolManager(size, tmpdir)
		bpm.RegisterDiskManager(disk.FileId(0), dm)

		// バッファプールを満杯にする
		pageId1 := dm.AllocatePage()
		pageId2 := dm.AllocatePage()
		pageId3 := dm.AllocatePage()

		bpm.AddPage(pageId1)
		bpm.AddPage(pageId2)
		bpm.AddPage(pageId3)

		// すべてのページの Referenced, IsDirty を false にする
		bpm.bufpool.BufferPages[0].Referenced = false
		bpm.bufpool.BufferPages[0].IsDirty = false
		bpm.bufpool.BufferPages[1].Referenced = false
		bpm.bufpool.BufferPages[1].IsDirty = false
		bpm.bufpool.BufferPages[2].Referenced = false
		bpm.bufpool.BufferPages[2].IsDirty = false

		// WHEN
		pageId4 := dm.AllocatePage()
		newPage, err := bpm.AddPage(pageId4)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, newPage)
		assert.Equal(t, pageId4, newPage.PageId)
		// 新しいページがページテーブルに追加されていることを確認
		_, ok := bpm.pageTable[pageId4]
		assert.True(t, ok)
		// 古いページ (pageId1) がページテーブルから削除されていることを確認
		_, ok = bpm.pageTable[pageId1]
		assert.False(t, ok)
	})
}

func TestFlushPage(t *testing.T) {
	t.Run("ページテーブル内にダーティーページが存在する場合", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		dm, _ := initDiskManager(t, tmpdir)
		bpm := NewBufferPoolManager(size, tmpdir)
		bpm.RegisterDiskManager(disk.FileId(0), dm)

		pageId1 := dm.AllocatePage()
		pageId2 := dm.AllocatePage()
		pageId3 := dm.AllocatePage()

		page1, _ := bpm.AddPage(pageId1)
		page2, _ := bpm.AddPage(pageId2)
		bpm.AddPage(pageId3)

		// ページにデータを書き込み、ダーティーにする
		page1.Page[0] = 11
		page1.IsDirty = true
		page2.Page[0] = 22
		page2.IsDirty = true

		// WHEN
		err := bpm.FlushPage()

		// THEN
		assert.NoError(t, err)
		assert.False(t, page1.IsDirty)
		assert.False(t, page2.IsDirty)

		// データがディスクに書き込まれていることを確認
		// バッファプールをクリアして、ディスクから読み直す
		bpm.bufpool.BufferPages[0].Referenced = false
		bpm.bufpool.BufferPages[1].Referenced = false
		bpm.bufpool.BufferPages[2].Referenced = false
		pageId4 := dm.AllocatePage()
		pageId5 := dm.AllocatePage()
		pageId6 := dm.AllocatePage()
		bpm.FetchPage(pageId4)
		bpm.FetchPage(pageId5)
		bpm.FetchPage(pageId6)

		// page1 と page2 を再度フェッチして、データが正しく読み出せることを確認
		reFetchedPage1, err := bpm.FetchPage(pageId1)
		assert.NoError(t, err)
		assert.Equal(t, byte(11), reFetchedPage1.Page[0])

		reFetchedPage2, err := bpm.FetchPage(pageId2)
		assert.NoError(t, err)
		assert.Equal(t, byte(22), reFetchedPage2.Page[0])
	})

	t.Run("ページテーブル内のすべてのページがダーティーでない場合", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		dm, _ := initDiskManager(t, tmpdir)
		bpm := NewBufferPoolManager(size, tmpdir)
		bpm.RegisterDiskManager(disk.FileId(0), dm)

		pageId1 := dm.AllocatePage()
		pageId2 := dm.AllocatePage()
		pageId3 := dm.AllocatePage()

		page1, _ := bpm.AddPage(pageId1)
		page2, _ := bpm.AddPage(pageId2)
		page3, _ := bpm.AddPage(pageId3)

		page1.IsDirty = false
		page2.IsDirty = false
		page3.IsDirty = false

		// WHEN
		err := bpm.FlushPage()

		// THEN
		assert.NoError(t, err)
		assert.False(t, page1.IsDirty)
		assert.False(t, page2.IsDirty)
		assert.False(t, page3.IsDirty)
	})
}

func TestUnRefPage(t *testing.T) {
	t.Run("指定されたページの参照ビットがクリアされる", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		dm, _ := initDiskManager(t, tmpdir)
		bpm := NewBufferPoolManager(size, tmpdir)
		bpm.RegisterDiskManager(disk.FileId(0), dm)
		pageId := dm.AllocatePage()

		bufferPage, err := bpm.AddPage(pageId)
		assert.NoError(t, err)

		bufferPage.Referenced = true

		// WHEN
		bpm.UnRefPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.False(t, bufferPage.Referenced)
	})
}

func TestBufferPoolManagerIntegration(t *testing.T) {
	t.Run("バッファプールの統合動作テスト (ページアクセス、ページ置換、参照ビット管理)", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		fileId := disk.FileId(0)
		dm, err := disk.NewDiskManager(fileId, path)
		assert.NoError(t, err)
		bpm := NewBufferPoolManager(3, tmpdir)
		bpm.RegisterDiskManager(fileId, dm)

		// ページを作成
		page1 := dm.AllocatePage()
		page2 := dm.AllocatePage()
		page3 := dm.AllocatePage()
		page4 := dm.AllocatePage()
		page5 := dm.AllocatePage()

		// 各ページにデータを書き込む (PageID と同じ値を書き込む)
		writeTestData := func(pageId disk.PageId, value byte) {
			data := make([]byte, disk.PAGE_SIZE)
			for i := range data {
				data[i] = value
			}
			err := dm.WritePageData(pageId, data)
			assert.NoError(t, err)
		}

		writeTestData(page1, byte(page1.PageNumber))
		writeTestData(page2, byte(page2.PageNumber))
		writeTestData(page3, byte(page3.PageNumber))
		writeTestData(page4, byte(page4.PageNumber))
		writeTestData(page5, byte(page5.PageNumber))

		// ===============================
		// ページアクセスのシミュレーション
		// ===============================

		// ### 1. page1, page2, page3 をフェッチ (バッファプールに読み込まれる)
		fetchedPage1, err := bpm.FetchPage(page1)
		assert.NoError(t, err)
		assert.Equal(t, byte(page1.PageNumber), fetchedPage1.Page[0])
		assert.True(t, fetchedPage1.Referenced)

		fetchedPage2, err := bpm.FetchPage(page2)
		assert.NoError(t, err)
		assert.Equal(t, byte(page2.PageNumber), fetchedPage2.Page[0])
		assert.True(t, fetchedPage2.Referenced)

		fetchedPage3, err := bpm.FetchPage(page3)
		assert.NoError(t, err)
		assert.Equal(t, byte(page3.PageNumber), fetchedPage3.Page[0])
		assert.True(t, fetchedPage3.Referenced)

		assert.Equal(t, 3, len(bpm.pageTable)) // バッファプールが満杯になっている

		// ### 2. page4 をアクセス (ページ置換発生)
		// すべてのページの Referenced を false にした後、page1 が置換される
		fetchedPage4, err := bpm.FetchPage(page4)
		assert.NoError(t, err)
		assert.Equal(t, byte(page4.PageNumber), fetchedPage4.Page[0])
		assert.True(t, fetchedPage4.Referenced)

		// page1 がページテーブルから削除される
		_, page1InBuffer := bpm.pageTable[page1]
		assert.False(t, page1InBuffer)

		// page4 がバッファプールに追加される
		_, ok := bpm.pageTable[page4]
		assert.True(t, ok)

		// page2, page3 の Referenced は false になる
		assert.False(t, fetchedPage2.Referenced)
		assert.False(t, fetchedPage3.Referenced)

		// ### 3. page5 をアクセス (ページ置換発生)
		// page2 が置換される (Referenced が false で最初に見つかるページ)
		fetchedPage5, err := bpm.FetchPage(page5)
		assert.NoError(t, err)
		assert.Equal(t, byte(page5.PageNumber), fetchedPage5.Page[0])
		assert.True(t, fetchedPage5.Referenced)

		// page2 がページテーブルから削除されることを確認
		_, page2InBuffer := bpm.pageTable[page2]
		assert.False(t, page2InBuffer)

		// page5 がバッファプールに追加される
		_, ok = bpm.pageTable[page5]
		assert.True(t, ok)

		// ### 4. page1 を再度アクセス
		// page1 がバッファから追い出されているため、再度ディスクから読み込まれる
		// page3 が置換される
		reFetchedPage1, err := bpm.FetchPage(page1)
		assert.NoError(t, err)
		assert.Equal(t, byte(page1.PageNumber), reFetchedPage1.Page[0])
		assert.True(t, reFetchedPage1.Referenced)

		// page3 がページテーブルから削除されることを確認
		_, page3InBuffer := bpm.pageTable[page3]
		assert.False(t, page3InBuffer)

		// page1 がバッファプールに存在する
		_, page1InBuffer = bpm.pageTable[page1]
		assert.True(t, page1InBuffer)

		// 最終的に page4, page5, page1 がバッファプールに存在
		assert.Equal(t, 3, len(bpm.pageTable))
		assert.Contains(t, bpm.pageTable, page4)
		assert.Contains(t, bpm.pageTable, page5)
		assert.Contains(t, bpm.pageTable, page1)
	})
}

func TestAllocateFileId(t *testing.T) {
	t.Run("FileId が順番に割り当てられる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		bpm := NewBufferPoolManager(10, tmpdir)

		// WHEN
		fileId1 := bpm.AllocateFileId()
		fileId2 := bpm.AllocateFileId()
		fileId3 := bpm.AllocateFileId()

		// THEN
		assert.Equal(t, disk.FileId(1), fileId1)
		assert.Equal(t, disk.FileId(2), fileId2)
		assert.Equal(t, disk.FileId(3), fileId3)
	})
}

func TestAllocatePageId(t *testing.T) {
	t.Run("登録された FileId に対して PageId が正しく割り当てられる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		bpm := NewBufferPoolManager(10, tmpdir)
		fileId := disk.FileId(1)
		path := filepath.Join(tmpdir, "test.db")
		dm, err := disk.NewDiskManager(fileId, path)
		assert.NoError(t, err)
		bpm.RegisterDiskManager(fileId, dm)

		// WHEN
		pageId1, err := bpm.AllocatePageId(fileId)
		pageId2, err := bpm.AllocatePageId(fileId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, fileId, pageId1.FileId)
		assert.Equal(t, fileId, pageId2.FileId)
		assert.Equal(t, disk.PageNumber(0), pageId1.PageNumber)
		assert.Equal(t, disk.PageNumber(1), pageId2.PageNumber)
	})

	t.Run("登録されていない FileId に対してエラーが返される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		bpm := NewBufferPoolManager(10, tmpdir)
		nonExistentFileId := disk.FileId(999)

		// WHEN
		pageId, err := bpm.AllocatePageId(nonExistentFileId)

		// THEN
		assert.Error(t, err)
		assert.Equal(t, disk.INVALID_PAGE_ID, pageId)
		assert.Contains(t, err.Error(), "DiskManager for FileId 999 not found")
	})

	t.Run("複数の FileId に対してそれぞれ独立した PageId が割り当てられる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		bpm := NewBufferPoolManager(10, tmpdir)

		fileId1 := disk.FileId(1)
		path1 := filepath.Join(tmpdir, "test1.db")
		dm1, err := disk.NewDiskManager(fileId1, path1)
		assert.NoError(t, err)
		bpm.RegisterDiskManager(fileId1, dm1)

		fileId2 := disk.FileId(2)
		path2 := filepath.Join(tmpdir, "test2.db")
		dm2, err := disk.NewDiskManager(fileId2, path2)
		assert.NoError(t, err)
		bpm.RegisterDiskManager(fileId2, dm2)

		// WHEN
		pageId1_1, err := bpm.AllocatePageId(fileId1)
		assert.NoError(t, err)
		pageId2_1, err := bpm.AllocatePageId(fileId2)
		assert.NoError(t, err)
		pageId1_2, err := bpm.AllocatePageId(fileId1)
		assert.NoError(t, err)
		pageId2_2, err := bpm.AllocatePageId(fileId2)
		assert.NoError(t, err)

		// THEN
		// FileId1 のページ
		assert.Equal(t, fileId1, pageId1_1.FileId)
		assert.Equal(t, fileId1, pageId1_2.FileId)
		assert.Equal(t, disk.PageNumber(0), pageId1_1.PageNumber)
		assert.Equal(t, disk.PageNumber(1), pageId1_2.PageNumber)

		// FileId2 のページ
		assert.Equal(t, fileId2, pageId2_1.FileId)
		assert.Equal(t, fileId2, pageId2_2.FileId)
		assert.Equal(t, disk.PageNumber(0), pageId2_1.PageNumber)
		assert.Equal(t, disk.PageNumber(1), pageId2_2.PageNumber)
	})
}

func initDiskManager(t *testing.T, tmpdir string) (*disk.DiskManager, disk.PageId) {
	path := filepath.Join(tmpdir, "test.db")
	dm, err := disk.NewDiskManager(disk.FileId(0), path)
	assert.NoError(t, err)
	pageId := dm.AllocatePage()

	// ページをディスクに書き込む (空のページ)
	emptyPage := make([]byte, disk.PAGE_SIZE)
	err = dm.WritePageData(pageId, emptyPage)
	assert.NoError(t, err)

	return dm, pageId
}