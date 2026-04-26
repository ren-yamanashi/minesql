package btree

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewIterator(t *testing.T) {
	t.Run("正常にイテレータを生成できる", func(t *testing.T) {
		// GIVEN
		var bufferPageMock buffer.BufferPage
		slotNum := 0

		// WHEN
		iterator := newIterator(nil, bufferPageMock, slotNum)

		// THEN
		assert.NotNil(t, iterator)
		assert.Equal(t, bufferPageMock, iterator.bufferPage)
		assert.Equal(t, slotNum, iterator.slotNum)
	})
}

func TestGet(t *testing.T) {
	t.Run("現在の key-value レコードが取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		dm := initDiskForIterator(t, tmpdir)
		bp := buffer.NewBufferPool(3, nil)
		bp.RegisterDisk(page.FileId(0), dm)

		record1 := node.NewRecord(nil, []byte("key1"), []byte("value1"))
		record2 := node.NewRecord(nil, []byte("key2"), []byte("value2"))
		record3 := node.NewRecord(nil, []byte("key3"), []byte("value3"))

		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))
		bufferPage := createLeafBufferPage(pageId, []node.Record{record1, record2, record3}, nil)

		_ = bp.AddPage(pageId)
		writeData, _ := bp.GetWritePageData(pageId)
		copy(writeData, bufferPage.Page)

		iterator := newIterator(bp, bufferPage, 1) // 2 番目のレコードを指している

		// WHEN
		record, ok := iterator.Get()

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte("key2"), record.KeyBytes())
		assert.Equal(t, []byte("value2"), record.NonKeyBytes())
	})

	t.Run("slotNum がレコード数以上の場合、false を返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		dm := initDiskForIterator(t, tmpdir)
		bp := buffer.NewBufferPool(3, nil)
		bp.RegisterDisk(page.FileId(0), dm)

		record1 := node.NewRecord(nil, []byte("key1"), []byte("value1"))
		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))
		bufferPage := createLeafBufferPage(pageId, []node.Record{record1}, nil)

		_ = bp.AddPage(pageId)
		writeData, _ := bp.GetWritePageData(pageId)
		copy(writeData, bufferPage.Page)

		iterator := newIterator(bp, bufferPage, 1) // レコード数 (1) と同じ slotNum

		// WHEN
		_, ok := iterator.Get()

		// THEN
		assert.False(t, ok)
	})
}

func TestNext(t *testing.T) {
	t.Run("次の key-value レコードの情報が取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		dm := initDiskForIterator(t, tmpdir)
		bp := buffer.NewBufferPool(3, nil)
		bp.RegisterDisk(page.FileId(0), dm)

		record1 := node.NewRecord(nil, []byte("key1"), []byte("value1"))
		record2 := node.NewRecord(nil, []byte("key2"), []byte("value2"))
		record3 := node.NewRecord(nil, []byte("key3"), []byte("value3"))

		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))
		bufferPage := createLeafBufferPage(pageId, []node.Record{record1, record2, record3}, nil)

		_ = bp.AddPage(pageId)
		writeData, _ := bp.GetWritePageData(pageId)
		copy(writeData, bufferPage.Page)

		iterator := newIterator(bp, bufferPage, 0)
		assert.Equal(t, 0, iterator.slotNum) // 最初のレコードを指している

		// WHEN
		record, ok, err := iterator.Next(bp)
		slotNum1 := iterator.slotNum
		record2Result, ok2, err2 := iterator.Next(bp)
		slotNum2 := iterator.slotNum

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte("key1"), record.KeyBytes())
		assert.Equal(t, []byte("value1"), record.NonKeyBytes())
		assert.Equal(t, 1, slotNum1) // 1 回目の Next() 後の slotNum
		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, []byte("key2"), record2Result.KeyBytes())
		assert.Equal(t, []byte("value2"), record2Result.NonKeyBytes())
		assert.Equal(t, 2, slotNum2) // 2 回目の Next() 後の slotNum
	})

	t.Run("レコードが残っていない場合、false を返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		dm := initDiskForIterator(t, tmpdir)
		bp := buffer.NewBufferPool(3, nil)
		bp.RegisterDisk(page.FileId(0), dm)

		record1 := node.NewRecord(nil, []byte("key1"), []byte("value1"))
		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))
		bufferPage := createLeafBufferPage(pageId, []node.Record{record1}, nil)

		_ = bp.AddPage(pageId)
		writeData, _ := bp.GetWritePageData(pageId)
		copy(writeData, bufferPage.Page)

		iterator := newIterator(bp, bufferPage, 1) // レコード数を超えた位置

		// WHEN
		_, ok, err := iterator.Next(bp)

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("ページをまたいで Next を呼ぶと次のページのレコードが取得できる", func(t *testing.T) {
		// GIVEN: 2 ページ構成 (各 1 レコード)
		tmpdir := t.TempDir()
		dm := initDiskForIterator(t, tmpdir)
		bp := buffer.NewBufferPool(3, nil)
		bp.RegisterDisk(page.FileId(0), dm)

		record1 := node.NewRecord(nil, []byte("key1"), []byte("value1"))
		nextPageId := page.NewPageId(page.FileId(0), page.PageNumber(1))
		bufferPage1 := createLeafBufferPage(page.NewPageId(page.FileId(0), page.PageNumber(0)), []node.Record{record1}, &nextPageId)

		record2 := node.NewRecord(nil, []byte("key2"), []byte("value2"))
		bufferPage2 := createLeafBufferPage(page.NewPageId(page.FileId(0), page.PageNumber(1)), []node.Record{record2}, nil)

		// 次のページをディスクに書き込む
		dm.AllocatePage() // page 1 を採番
		err := dm.WritePageData(page.NewPageId(page.FileId(0), page.PageNumber(1)), bufferPage2.Page)
		assert.NoError(t, err)

		// ページ 1 をバッファプールに追加
		page0Id := page.NewPageId(page.FileId(0), page.PageNumber(0))
		err = bp.AddPage(page0Id)
		assert.NoError(t, err)
		addedPage1WriteData, err := bp.GetWritePageData(page0Id)
		assert.NoError(t, err)
		copy(addedPage1WriteData, bufferPage1.Page)

		addedPage1, err := bp.FetchPage(page0Id)
		assert.NoError(t, err)
		iterator := newIterator(bp, *addedPage1, 0)

		// WHEN: 2 回 Next を呼ぶ (1 回目でページ遷移が発生)
		rec1, ok1, err1 := iterator.Next(bp)
		rec2, ok2, err2 := iterator.Next(bp)

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, []byte("key1"), rec1.KeyBytes())

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, []byte("key2"), rec2.KeyBytes())
	})
}

func TestAdvance(t *testing.T) {
	t.Run("現在のページ内に、まだ次の key-value レコードがある場合は何もしない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		dm := initDiskForIterator(t, tmpdir)
		bp := buffer.NewBufferPool(3, nil)
		bp.RegisterDisk(page.FileId(0), dm)

		record1 := node.NewRecord(nil, []byte("key1"), []byte("value1"))
		record2 := node.NewRecord(nil, []byte("key2"), []byte("value2"))
		record3 := node.NewRecord(nil, []byte("key3"), []byte("value3"))

		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))
		bufferPage := createLeafBufferPage(pageId, []node.Record{record1, record2, record3}, nil)

		_ = bp.AddPage(pageId)
		writeData, _ := bp.GetWritePageData(pageId)
		copy(writeData, bufferPage.Page)

		iterator := newIterator(bp, bufferPage, 0)

		// WHEN
		err := iterator.Advance(bp)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, pageId, iterator.bufferPage.PageId) // ページは変わらない
	})

	t.Run("現在のページ内に、次の key-value レコードがないが、次のページも存在しない場合は何もしない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		dm := initDiskForIterator(t, tmpdir)
		bp := buffer.NewBufferPool(3, nil)
		bp.RegisterDisk(page.FileId(0), dm)

		record1 := node.NewRecord(nil, []byte("key1"), []byte("value1"))
		record2 := node.NewRecord(nil, []byte("key2"), []byte("value2"))

		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))
		bufferPage := createLeafBufferPage(pageId, []node.Record{record1, record2}, nil)

		_ = bp.AddPage(pageId)
		writeData, _ := bp.GetWritePageData(pageId)
		copy(writeData, bufferPage.Page)

		iterator := newIterator(bp, bufferPage, 1) // 最後のレコードを指している

		// WHEN
		err := iterator.Advance(bp)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(0)), iterator.bufferPage.PageId) // ページは変わらない
	})

	t.Run("現在のページ内に次の key-value レコードがなく、次のページが存在する場合は、次のページに移動する (古いページの参照ビットがクリアされ、次のページの先頭にポインタが置かれる)", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		dm := initDiskForIterator(t, tmpdir)
		bp := buffer.NewBufferPool(3, nil)
		bp.RegisterDisk(page.FileId(0), dm)

		// 最初のページ
		record1 := node.NewRecord(nil, []byte("key1"), []byte("value1"))
		record2 := node.NewRecord(nil, []byte("key2"), []byte("value2"))
		nextPageId := page.NewPageId(page.FileId(0), page.PageNumber(1))
		bufferPage1 := createLeafBufferPage(page.NewPageId(page.FileId(0), page.PageNumber(0)), []node.Record{record1, record2}, &nextPageId)

		// 次のページ
		record3 := node.NewRecord(nil, []byte("key3"), []byte("value3"))
		record4 := node.NewRecord(nil, []byte("key4"), []byte("value4"))
		bufferPage2 := createLeafBufferPage(page.NewPageId(page.FileId(0), page.PageNumber(1)), []node.Record{record3, record4}, nil)

		// 次のページをディスクに書き込む
		err := dm.WritePageData(page.NewPageId(page.FileId(0), page.PageNumber(1)), bufferPage2.Page)
		assert.NoError(t, err)

		// ページ 1 をバッファプールに追加
		page0Id := page.NewPageId(page.FileId(0), page.PageNumber(0))
		err = bp.AddPage(page0Id)
		assert.NoError(t, err)
		addedPage1WriteData, err := bp.GetWritePageData(page0Id)
		assert.NoError(t, err)
		copy(addedPage1WriteData, bufferPage1.Page)

		addedPage1, err := bp.FetchPage(page0Id)
		assert.NoError(t, err)
		iterator := newIterator(bp, *addedPage1, 1) // 最後のレコードを指している

		// WHEN
		err = iterator.Advance(bp)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(1)), iterator.bufferPage.PageId)
		assert.Equal(t, 0, iterator.slotNum) // 次のページの先頭
	})
}

// リーフノードを含む BufferPage を作成する
func createLeafBufferPage(pageId page.PageId, records []node.Record, nextPageId *page.PageId) buffer.BufferPage {
	bufpool := buffer.NewBufferPage(pageId)

	leaf := node.NewLeaf(page.NewPage(bufpool.Page).Body)
	leaf.Initialize()

	// レコードを挿入
	for i, record := range records {
		if !leaf.Insert(i, record) {
			panic("failed to insert record")
		}
	}

	// nextPageId を設定
	leaf.SetNextPageId(nextPageId)

	return *bufpool
}

func initDiskForIterator(t *testing.T, tmpdir string) *file.Disk {
	path := filepath.Join(tmpdir, "iterator_test.db")
	dm, err := file.NewDisk(page.FileId(0), path)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	dm.AllocatePage()
	return dm
}
