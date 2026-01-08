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
		assert.Equal(t, pageId, fetchedPage.PageId)
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
		assert.Equal(t, pageId, bufferPage.PageId)
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
		assert.Equal(t, pageId4, newPage.PageId)
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
		assert.Equal(t, pageId4, newPage.PageId)
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

func initDiskManager(t *testing.T) (*disk.DiskManager, disk.PageId) {
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
	nextPageId             disk.PageId
}

func NewDiskManagerSpy() *DiskManagerSpy {
	return &DiskManagerSpy{
		nextPageId: 0,
	}
}

func (spy *DiskManagerSpy) ReadPageData(id disk.PageId, data []byte) error {
	spy.readPageDataCallCount++
	// データは読み込まないが、エラーも返さない
	return nil
}

func (spy *DiskManagerSpy) WritePageData(id disk.PageId, data []byte) error {
	spy.writePageDataCallCount++
	return nil
}

func (spy *DiskManagerSpy) AllocatePage() disk.PageId {
	id := spy.nextPageId
	spy.nextPageId++
	return id
}

func (spy *DiskManagerSpy) Sync() error {
	return nil
}
