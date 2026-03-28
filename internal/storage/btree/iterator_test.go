package btree

import (
	"minesql/internal/storage/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
	"minesql/internal/storage/page"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewIterator(t *testing.T) {
	t.Run("正常にイテレータを生成できる", func(t *testing.T) {
		// GIVEN
		var bufferPageMock bufferpool.BufferPage
		slotNum := 0

		// WHEN
		iterator := newIterator(bufferPageMock, slotNum)

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
		bp := bufferpool.NewBufferPool(3)
		bp.RegisterDisk(page.FileId(0), dm)

		record1 := node.NewRecord(nil, []byte("key1"), []byte("value1"))
		record2 := node.NewRecord(nil, []byte("key2"), []byte("value2"))
		record3 := node.NewRecord(nil, []byte("key3"), []byte("value3"))

		bufferPage := createLeafBufferPage(page.NewPageId(page.FileId(0), page.PageNumber(0)), []node.Record{record1, record2, record3}, nil)
		iterator := newIterator(bufferPage, 1) // 2 番目のレコードを指している

		// WHEN
		record, ok := iterator.Get()

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte("key2"), record.KeyBytes())
		assert.Equal(t, []byte("value2"), record.NonKeyBytes())
	})
}

func TestNext(t *testing.T) {
	t.Run("次の key-value レコードの情報が取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		dm := initDiskForIterator(t, tmpdir)
		bp := bufferpool.NewBufferPool(3)
		bp.RegisterDisk(page.FileId(0), dm)

		record1 := node.NewRecord(nil, []byte("key1"), []byte("value1"))
		record2 := node.NewRecord(nil, []byte("key2"), []byte("value2"))
		record3 := node.NewRecord(nil, []byte("key3"), []byte("value3"))

		bufferPage := createLeafBufferPage(page.NewPageId(page.FileId(0), page.PageNumber(0)), []node.Record{record1, record2, record3}, nil)
		iterator := newIterator(bufferPage, 0)
		assert.Equal(t, 0, iterator.slotNum) // 最初のレコードを指している

		// WHEN
		record, ok, err := iterator.Next(bp)
		bufferId1 := iterator.slotNum
		record2Result, ok2, err2 := iterator.Next(bp)
		bufferId2 := iterator.slotNum

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte("key1"), record.KeyBytes())
		assert.Equal(t, []byte("value1"), record.NonKeyBytes())
		assert.Equal(t, 1, bufferId1) // 1 回目の Next() 後の bufferId
		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, []byte("key2"), record2Result.KeyBytes())
		assert.Equal(t, []byte("value2"), record2Result.NonKeyBytes())
		assert.Equal(t, 2, bufferId2) // 2 回目の Next() 後の bufferId
	})
}

func TestAdvance(t *testing.T) {
	t.Run("現在のページ内に、まだ次の key-value レコードがある場合は何もしない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		dm := initDiskForIterator(t, tmpdir)
		bp := bufferpool.NewBufferPool(3)
		bp.RegisterDisk(page.FileId(0), dm)

		record1 := node.NewRecord(nil, []byte("key1"), []byte("value1"))
		record2 := node.NewRecord(nil, []byte("key2"), []byte("value2"))
		record3 := node.NewRecord(nil, []byte("key3"), []byte("value3"))

		bufferPage := createLeafBufferPage(page.NewPageId(page.FileId(0), page.PageNumber(0)), []node.Record{record1, record2, record3}, nil)
		iterator := newIterator(bufferPage, 0)

		// WHEN
		err := iterator.Advance(bp)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(0)), iterator.bufferPage.PageId) // ページは変わらない
	})

	t.Run("現在のページ内に、次の key-value レコードがないが、次のページも存在しない場合は何もしない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		dm := initDiskForIterator(t, tmpdir)
		bp := bufferpool.NewBufferPool(3)
		bp.RegisterDisk(page.FileId(0), dm)

		record1 := node.NewRecord(nil, []byte("key1"), []byte("value1"))
		record2 := node.NewRecord(nil, []byte("key2"), []byte("value2"))

		bufferPage := createLeafBufferPage(page.NewPageId(page.FileId(0), page.PageNumber(0)), []node.Record{record1, record2}, nil)
		iterator := newIterator(bufferPage, 1) // 最後のレコードを指している

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
		bp := bufferpool.NewBufferPool(3)
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
		err := dm.WritePageData(page.NewPageId(page.FileId(0), page.PageNumber(1)), bufferPage2.GetReadData())
		assert.NoError(t, err)

		// ページ 1 をバッファプールに追加
		addedPage1, err := bp.AddPage(page.NewPageId(page.FileId(0), page.PageNumber(0)))
		assert.NoError(t, err)
		copy(addedPage1.GetWriteData(), bufferPage1.GetReadData())

		iterator := newIterator(*addedPage1, 1) // 最後のレコードを指している

		// WHEN
		err = iterator.Advance(bp)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(1)), iterator.bufferPage.PageId)
		assert.Equal(t, 0, iterator.slotNum) // 次のページの先頭
	})
}

// リーフノードを含む BufferPage を作成する
func createLeafBufferPage(pageId page.PageId, records []node.Record, nextPageId *page.PageId) bufferpool.BufferPage {
	bufpool := bufferpool.NewBufferPage(pageId)

	leafNode := node.NewLeafNode(bufpool.GetWriteData())
	leafNode.Initialize()

	// レコードを挿入
	for i, record := range records {
		if !leafNode.Insert(i, record) {
			panic("failed to insert record")
		}
	}

	// nextPageId を設定
	leafNode.SetNextPageId(nextPageId)

	return *bufpool
}

func initDiskForIterator(t *testing.T, tmpdir string) *disk.Disk {
	path := filepath.Join(tmpdir, "iterator_test.db")
	dm, err := disk.NewDisk(page.FileId(0), path)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	dm.AllocatePage()
	return dm
}
