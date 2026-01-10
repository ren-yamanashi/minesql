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
		dm, err := disk.NewDiskManager(path)
		assert.NoError(t, err)

		// WHEN
		bpm := NewBufferPoolManager(dm, size)

		// THEN
		assert.NotNil(t, bpm)
		assert.Equal(t, dm, bpm.DiskManager)
		assert.Equal(t, size, bpm.bufpool.MaxBufferSize)
		assert.Equal(t, size, len(bpm.bufpool.BufferPages))
		assert.Equal(t, 0, len(bpm.pageTable))
	})
}

func TestFetchPage(t *testing.T) {
	t.Run("指定されたページがページテーブルに存在する場合、ディスク I/O は発生しない", func(t *testing.T) {
		// GIVEN
		size := 3
		dmSpy := NewDiskManagerSpy()
		pageId := dmSpy.AllocatePage()
		bpm := NewBufferPoolManager(dmSpy, size)

		bufferPage, err := bpm.AddPage(pageId)
		assert.NoError(t, err)

		// スパイのカウンタをリセット (AddPage での呼び出しをカウントしないため)
		dmSpy.readPageDataCallCount = 0
		dmSpy.writePageDataCallCount = 0

		// WHEN
		fetchedPage, err := bpm.FetchPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, bufferPage, fetchedPage)
		assert.Equal(t, bpm.pageTable[pageId], BufferId(0))
		assert.True(t, fetchedPage.Referenced)
		assert.Equal(t, 0, dmSpy.readPageDataCallCount)
		assert.Equal(t, 0, dmSpy.writePageDataCallCount)
	})

	t.Run("指定されたページがページテーブルに存在しない場合、ディスクからページが読み込まれる", func(t *testing.T) {
		// GIVEN
		size := 3
		dmSpy := NewDiskManagerSpy()
		pageId := dmSpy.AllocatePage()
		bpm := NewBufferPoolManager(dmSpy, size)

		// WHEN
		fetchedPage, err := bpm.FetchPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, fetchedPage)
		assert.Equal(t, pageId, fetchedPage.OldPageId)
		assert.True(t, fetchedPage.Referenced)
		assert.False(t, fetchedPage.IsDirty)
		assert.Equal(t, BufferId(0), bpm.pageTable[pageId])
		assert.Equal(t, 1, dmSpy.readPageDataCallCount)
		assert.Equal(t, 0, dmSpy.writePageDataCallCount)
	})
}

func TestAddPage(t *testing.T) {
	t.Run("バッファプールに空きがある場合、新しいページが追加される", func(t *testing.T) {
		// GIVEN
		size := 3
		dmSpy := NewDiskManagerSpy()
		pageId := dmSpy.AllocatePage()
		bpm := NewBufferPoolManager(dmSpy, size)

		// スパイのカウンタをリセット (AllocatePage での呼び出しをカウントしないため)
		dmSpy.readPageDataCallCount = 0
		dmSpy.writePageDataCallCount = 0

		// WHEN
		bufferPage, err := bpm.AddPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, bufferPage)
		assert.Equal(t, pageId, bufferPage.OldPageId)
		bufferId, ok := bpm.pageTable[pageId]
		assert.True(t, ok)
		assert.Equal(t, BufferId(0), bufferId)
		assert.Equal(t, 0, dmSpy.readPageDataCallCount)
		assert.Equal(t, 0, dmSpy.writePageDataCallCount)
	})

	t.Run("バッファプールに空きがない場合、かつ該当のページがダーティーな場合、一度ページの内容をディスクに書き込んだ後、ページが置換される", func(t *testing.T) {
		// GIVEN
		size := 3
		dmSpy := NewDiskManagerSpy()
		bpm := NewBufferPoolManager(dmSpy, size)

		// バッファプールを満杯にする
		pageId1 := dmSpy.AllocatePage()
		pageId2 := dmSpy.AllocatePage()
		pageId3 := dmSpy.AllocatePage()

		page1, _ := bpm.AddPage(pageId1)
		bpm.AddPage(pageId2)
		bpm.AddPage(pageId3)

		page1.IsDirty = true

		// すべてのページの Referenced を false にして、最初のページが選ばれるようにする
		bpm.bufpool.BufferPages[0].Referenced = false
		bpm.bufpool.BufferPages[1].Referenced = false
		bpm.bufpool.BufferPages[2].Referenced = false

		dmSpy.writePageDataCallCount = 0

		// WHEN
		pageId4 := dmSpy.AllocatePage()
		newPage, err := bpm.AddPage(pageId4)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, newPage)
		assert.Equal(t, pageId4, newPage.OldPageId)
		// ダーティーページの書き込みが発生していることを確認
		assert.Equal(t, 1, dmSpy.writePageDataCallCount)
		// 新しいページがページテーブルに追加されていることを確認
		_, ok := bpm.pageTable[pageId4]
		assert.True(t, ok)
		// 古いページ (pageId1) がページテーブルから削除されていることを確認
		_, ok = bpm.pageTable[pageId1]
		assert.False(t, ok)
	})

	t.Run("バッファプールに空きがない場合、かつ該当のページがダーティーでない場合、そのままページが置換される", func(t *testing.T) {
		// GIVEN
		size := 3
		dmSpy := NewDiskManagerSpy()
		bpm := NewBufferPoolManager(dmSpy, size)

		// バッファプールを満杯にする
		pageId1 := dmSpy.AllocatePage()
		pageId2 := dmSpy.AllocatePage()
		pageId3 := dmSpy.AllocatePage()

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

		dmSpy.writePageDataCallCount = 0

		// WHEN
		pageId4 := dmSpy.AllocatePage()
		newPage, err := bpm.AddPage(pageId4)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, newPage)
		assert.Equal(t, pageId4, newPage.OldPageId)
		// ダーティーでないため、ディスクへの書き込みは発生しない
		assert.Equal(t, 0, dmSpy.writePageDataCallCount)
		// 新しいページがページテーブルに追加されていることを確認
		_, ok := bpm.pageTable[pageId4]
		assert.True(t, ok)
		// 古いページ (pageId1) がページテーブルから削除されていることを確認
		_, ok = bpm.pageTable[pageId1]
		assert.False(t, ok)
	})
}

func TestFlushPage(t *testing.T) {
	t.Run("ページテーブル内にダーティーページが存在する場合、そのページがディスクに書き込まれる", func(t *testing.T) {
		// GIVEN
		size := 3
		dmSpy := NewDiskManagerSpy()
		bpm := NewBufferPoolManager(dmSpy, size)

		pageId1 := dmSpy.AllocatePage()
		pageId2 := dmSpy.AllocatePage()
		pageId3 := dmSpy.AllocatePage()

		page1, _ := bpm.AddPage(pageId1)
		page2, _ := bpm.AddPage(pageId2)
		bpm.AddPage(pageId3)

		page1.IsDirty = true
		page2.IsDirty = true

		dmSpy.writePageDataCallCount = 0

		// WHEN
		err := bpm.FlushPage()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, dmSpy.writePageDataCallCount)
		assert.False(t, page1.IsDirty)
		assert.False(t, page2.IsDirty)
	})

	t.Run("ページテーブル内のすべてのページがダーティーでない場合、ディスクへの書き込みは発生しない", func(t *testing.T) {
		// GIVEN
		size := 3
		dmSpy := NewDiskManagerSpy()
		bpm := NewBufferPoolManager(dmSpy, size)

		pageId1 := dmSpy.AllocatePage()
		pageId2 := dmSpy.AllocatePage()
		pageId3 := dmSpy.AllocatePage()

		bpm.AddPage(pageId1)
		bpm.AddPage(pageId2)
		bpm.AddPage(pageId3)

		dmSpy.writePageDataCallCount = 0

		// WHEN
		err := bpm.FlushPage()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 0, dmSpy.writePageDataCallCount)
	})
}

func TestUnRefPage(t *testing.T) {
	t.Run("指定されたページの参照ビットがクリアされる", func(t *testing.T) {
		// GIVEN
		size := 3
		dmSpy := NewDiskManagerSpy()
		pageId := dmSpy.AllocatePage()
		bpm := NewBufferPoolManager(dmSpy, size)

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

func initDiskManager(t *testing.T) (*disk.DiskManager, disk.OldPageId) {
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "test.db")
	dm, err := disk.NewDiskManager(path)
	assert.NoError(t, err)
	pageId := dm.AllocatePage()
	return dm, pageId
}

type DiskManagerSpy struct {
	readPageDataCallCount  int
	writePageDataCallCount int
	nextPageId             disk.OldPageId
}

func NewDiskManagerSpy() *DiskManagerSpy {
	return &DiskManagerSpy{
		nextPageId: 0,
	}
}

func (spy *DiskManagerSpy) ReadPageData(id disk.OldPageId, data []byte) error {
	spy.readPageDataCallCount++
	// データは読み込まないが、エラーも返さない
	return nil
}

func (spy *DiskManagerSpy) WritePageData(id disk.OldPageId, data []byte) error {
	spy.writePageDataCallCount++
	return nil
}

func (spy *DiskManagerSpy) AllocatePage() disk.OldPageId {
	id := spy.nextPageId
	spy.nextPageId++
	return id
}

func (spy *DiskManagerSpy) Sync() error {
	return nil
}

func TestBufferPoolManagerIntegration(t *testing.T) {
	t.Run("バッファプールの統合動作テスト (ページアクセス、ページ置換、参照ビット管理)", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		dm, err := disk.NewDiskManager(path)
		assert.NoError(t, err)
		bpm := NewBufferPoolManager(dm, 3)

		// ページを作成
		page1 := dm.AllocatePage()
		page2 := dm.AllocatePage()
		page3 := dm.AllocatePage()
		page4 := dm.AllocatePage()
		page5 := dm.AllocatePage()

		// 各ページにデータを書き込む (PageID と同じ値を書き込む)
		writeTestData := func(pageId disk.OldPageId, value byte) {
			data := make([]byte, disk.PAGE_SIZE)
			for i := range data {
				data[i] = value
			}
			err := dm.WritePageData(pageId, data)
			assert.NoError(t, err)
		}

		writeTestData(page1, byte(page1))
		writeTestData(page2, byte(page2))
		writeTestData(page3, byte(page3))
		writeTestData(page4, byte(page4))
		writeTestData(page5, byte(page5))

		// ===============================
		// ページアクセスのシミュレーション
		// ===============================

		// ### 1. page1, page2, page3 をフェッチ (バッファプールに読み込まれる)
		fetchedPage1, err := bpm.FetchPage(page1)
		assert.NoError(t, err)
		assert.Equal(t, byte(page1), fetchedPage1.Page[0])
		assert.True(t, fetchedPage1.Referenced)

		fetchedPage2, err := bpm.FetchPage(page2)
		assert.NoError(t, err)
		assert.Equal(t, byte(page2), fetchedPage2.Page[0])
		assert.True(t, fetchedPage2.Referenced)

		fetchedPage3, err := bpm.FetchPage(page3)
		assert.NoError(t, err)
		assert.Equal(t, byte(page3), fetchedPage3.Page[0])
		assert.True(t, fetchedPage3.Referenced)

		assert.Equal(t, 3, len(bpm.pageTable)) // バッファプールが満杯になっている

		// ### 2. page4 をアクセス (ページ置換発生)
		// すべてのページの Referenced を false にした後、page1 が置換される
		fetchedPage4, err := bpm.FetchPage(page4)
		assert.NoError(t, err)
		assert.Equal(t, byte(page4), fetchedPage4.Page[0])
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
		assert.Equal(t, byte(page5), fetchedPage5.Page[0])
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
		assert.Equal(t, byte(page1), reFetchedPage1.Page[0])
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
