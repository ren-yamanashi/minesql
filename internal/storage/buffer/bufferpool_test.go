package buffer

import (
	"encoding/binary"
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
		bp := NewBufferPool(size, nil)
		bp.RegisterDisk(fileId, disk)

		// THEN
		assert.NotNil(t, bp)
		registeredDm, err := bp.GetDisk(fileId)
		assert.NoError(t, err)
		assert.Equal(t, disk, registeredDm)
		assert.Equal(t, size, bp.maxBufferSize)
		assert.Equal(t, 0, len(bp.bufferPages))
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

		bp := NewBufferPool(size, nil)

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

		bp := NewBufferPool(size, nil)
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
		bp := NewBufferPool(size, nil)

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
		bp := NewBufferPool(10, nil)
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
		bp := NewBufferPool(10, nil)
		nonExistentFileId := page.FileId(999)

		// WHEN
		pageId, err := bp.AllocatePageId(nonExistentFileId)

		// THEN
		assert.Error(t, err)
		assert.Equal(t, page.InvalidPageId, pageId)
		assert.Contains(t, err.Error(), "disk for FileId 999 not found")
	})

	t.Run("複数の FileId に対してそれぞれ独立した PageId が割り当てられる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		bp := NewBufferPool(10, nil)

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
		bp := NewBufferPool(size, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		fetchedPage, err := bp.FetchPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, pageId, fetchedPage.PageId)
		assert.Equal(t, BufferId(0), bp.pageTable[pageId])
	})

	t.Run("指定されたページがページテーブルに存在しない場合、ディスクからページが読み込まれる", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		disk, pageId := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(size, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		// WHEN
		fetchedPage, err := bp.FetchPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, fetchedPage)
		assert.Equal(t, pageId, fetchedPage.PageId)
		assert.False(t, fetchedPage.IsDirty)
		assert.Equal(t, BufferId(0), bp.pageTable[pageId])
	})
}

func TestAddPage(t *testing.T) {
	t.Run("バッファプールに空きがある場合、新しいページが追加される", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(size, nil)
		bp.RegisterDisk(page.FileId(0), disk)
		pageId := disk.AllocatePage()

		// WHEN
		err := bp.AddPage(pageId)

		// THEN
		assert.NoError(t, err)
		bufferId, ok := bp.pageTable[pageId]
		assert.True(t, ok)
		assert.Equal(t, BufferId(0), bufferId)
	})

	t.Run("バッファプールに空きがない場合、かつ該当のページがダーティーな場合、一度ページの内容をディスクに書き込んだ後、ページが置換される", func(t *testing.T) {
		// GIVEN
		size := 3
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(size, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		// バッファプールを満杯にする
		pageId1 := disk.AllocatePage()
		pageId2 := disk.AllocatePage()
		pageId3 := disk.AllocatePage()

		err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		err = bp.AddPage(pageId2)
		assert.NoError(t, err)
		err = bp.AddPage(pageId3)
		assert.NoError(t, err)

		// page1 にデータを書き込み、ダーティーにする
		data1, _ := bp.GetWritePageData(pageId1)
		data1[0] = 99

		// pageId1 を最後に参照解除して、LRU の末尾に配置し最初に追い出されるようにする
		bp.UnRefPage(pageId2)
		bp.UnRefPage(pageId3)
		bp.UnRefPage(pageId1)

		// WHEN
		pageId4 := disk.AllocatePage()
		err = bp.AddPage(pageId4)

		// THEN
		assert.NoError(t, err)
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
		bp := NewBufferPool(size, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		// バッファプールを満杯にする
		pageId1 := disk.AllocatePage()
		pageId2 := disk.AllocatePage()
		pageId3 := disk.AllocatePage()

		err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		err = bp.AddPage(pageId2)
		assert.NoError(t, err)
		err = bp.AddPage(pageId3)
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
		err = bp.AddPage(pageId4)

		// THEN
		assert.NoError(t, err)
		_, ok := bp.pageTable[pageId4]
		assert.True(t, ok)
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

		// page2 のみ参照を解除
		bp.UnRefPage(pageId2)

		// WHEN: 新しいページを追加 (追い出しが発生)
		pageId4 := disk.AllocatePage()
		err = bp.AddPage(pageId4)
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

func TestAddPageEvictionWithRedoLogNil(t *testing.T) {
	t.Run("redoLog が nil でもダーティーページの追い出しが正常に行われる", func(t *testing.T) {
		// GIVEN: redoLog なしのバッファプール
		tmpdir := t.TempDir()
		bp := NewBufferPool(3, nil)
		disk, _ := file.NewDisk(page.FileId(0), filepath.Join(tmpdir, "test.db"))
		bp.RegisterDisk(page.FileId(0), disk)

		pageId1 := disk.AllocatePage()
		pageId2 := disk.AllocatePage()
		pageId3 := disk.AllocatePage()

		_ = bp.AddPage(pageId1)
		_ = bp.AddPage(pageId2)
		_ = bp.AddPage(pageId3)

		// page1 をダーティーにする
		data1, _ := bp.GetWritePageData(pageId1)
		data1[0] = 0xBB

		// page1 を最後に参照解除して、最初に追い出されるようにする
		bp.UnRefPage(pageId2)
		bp.UnRefPage(pageId3)
		bp.UnRefPage(pageId1)

		// WHEN: 新しいページを追加 (page1 が追い出される)
		pageId4 := disk.AllocatePage()
		err := bp.AddPage(pageId4)
		assert.NoError(t, err)

		// THEN: page1 のデータがディスクに書き込まれている
		reFetched, err := bp.FetchPage(pageId1)
		assert.NoError(t, err)
		assert.Equal(t, byte(0xBB), reFetched.Page[0])
	})
}

func TestAddPageEvictionWithFlushList(t *testing.T) {
	t.Run("ダーティーページ追い出し時にフラッシュリストから除外される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		bp := NewBufferPool(3, nil)
		disk, _ := file.NewDisk(page.FileId(0), filepath.Join(tmpdir, "test.db"))
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

		// page1 をダーティーにしてフラッシュリストに追加
		data1, _ := bp.GetWritePageData(pageId1)
		data1[0] = 0xAA
		assert.True(t, bp.flushList.Contains(pageId1))

		// pageId1 を最後に参照解除して、最初に追い出されるようにする
		bp.UnRefPage(pageId2)
		bp.UnRefPage(pageId3)
		bp.UnRefPage(pageId1)

		// WHEN: 新しいページを追加 (pageId1 が追い出される)
		pageId4 := disk.AllocatePage()
		err = bp.AddPage(pageId4)
		assert.NoError(t, err)

		// THEN: フラッシュリストから除外されている
		assert.False(t, bp.flushList.Contains(pageId1))
	})

	t.Run("追い出し時に Page LSN > FlushedLSN なら REDO ログがフラッシュされる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := NewBufferPool(3, rl)
		disk, _ := file.NewDisk(page.FileId(0), filepath.Join(tmpdir, "test.db"))
		bp.RegisterDisk(page.FileId(0), disk)

		pageId1 := disk.AllocatePage()
		pageId2 := disk.AllocatePage()
		pageId3 := disk.AllocatePage()

		err = bp.AddPage(pageId1)
		assert.NoError(t, err)
		err = bp.AddPage(pageId2)
		assert.NoError(t, err)
		err = bp.AddPage(pageId3)
		assert.NoError(t, err)

		// page1 をダーティーにして、Page LSN を設定 (REDO ログバッファにレコードを追加)
		writeData, _ := bp.GetWritePageData(pageId1)
		lsn := rl.AppendPageCopy(1, pageId1, writeData)
		pg := page.NewPage(writeData)
		binary.BigEndian.PutUint32(pg.Header, uint32(lsn))

		// FlushedLSN < Page LSN であることを確認
		assert.Greater(t, lsn, rl.FlushedLSN())

		// pageId1 を最後に参照解除して、最初に追い出されるようにする
		bp.UnRefPage(pageId2)
		bp.UnRefPage(pageId3)
		bp.UnRefPage(pageId1)

		// WHEN: 新しいページを追加 (pageId1 が追い出される)
		pageId4 := disk.AllocatePage()
		err = bp.AddPage(pageId4)
		assert.NoError(t, err)

		// THEN: REDO ログがフラッシュされている (FlushedLSN が更新されている)
		assert.Equal(t, lsn, rl.FlushedLSN())
	})
}

func TestGetWritePageData(t *testing.T) {
	t.Run("ページデータを取得して書き込める", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(5, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId := disk.AllocatePage()
		err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		data, err := bp.GetWritePageData(pageId)
		assert.NoError(t, err)
		data[0] = 0xAB

		// THEN: 書き込んだデータが反映されている
		readData, err := bp.GetReadPageData(pageId)
		assert.NoError(t, err)
		assert.Equal(t, byte(0xAB), readData[0])
	})

	t.Run("ページが自動的にダーティーになりフラッシュリストに追加される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(5, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId := disk.AllocatePage()
		err := bp.AddPage(pageId)
		assert.NoError(t, err)

		assert.Equal(t, 0, bp.FlushListSize())

		// WHEN
		_, err = bp.GetWritePageData(pageId)
		assert.NoError(t, err)

		// THEN
		p, _ := bp.FetchPage(pageId)
		assert.True(t, p.IsDirty)
		assert.Equal(t, 1, bp.FlushListSize())
	})

	t.Run("既にダーティーなページを再取得してもフラッシュリストに重複追加されない", func(t *testing.T) {
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
		_, err = bp.GetWritePageData(pageId)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, 1, bp.FlushListSize())
	})
}

func TestGetReadPageData(t *testing.T) {
	t.Run("ページデータを読み込める", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(5, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId := disk.AllocatePage()
		err := bp.AddPage(pageId)
		assert.NoError(t, err)
		data, _ := bp.GetWritePageData(pageId)
		data[0] = 0xCD

		// WHEN
		readData, err := bp.GetReadPageData(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(0xCD), readData[0])
	})

	t.Run("ページがダーティーにならずフラッシュリストに追加されない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		disk, _ := createEmptyDisk(t, tmpdir)
		bp := NewBufferPool(5, nil)
		bp.RegisterDisk(page.FileId(0), disk)

		pageId := disk.AllocatePage()
		err := bp.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		_, err = bp.GetReadPageData(pageId)
		assert.NoError(t, err)

		// THEN
		p, _ := bp.FetchPage(pageId)
		assert.False(t, p.IsDirty)
		assert.Equal(t, 0, bp.FlushListSize())
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
		bp := NewBufferPool(3, nil)
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
