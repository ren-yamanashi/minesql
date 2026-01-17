package btree

import (
	"minesql/internal/storage/access/btree/node"
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
		bufferId := 0

		// WHEN
		iterator := newIterator(bufferPageMock, bufferId)

		// THEN
		assert.NotNil(t, iterator)
		assert.Equal(t, bufferPageMock, iterator.bufferPage)
		assert.Equal(t, bufferId, iterator.bufferId)
	})
}

func TestGet(t *testing.T) {
	t.Run("現在の key-value ペアが取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		dm, _ := initDiskManagerForIterator(t, tmpdir)
		bpm := bufferpool.NewBufferPoolManager(3)
		bpm.RegisterDiskManager(page.FileId(0), dm)

		pair1 := node.NewPair([]byte("key1"), []byte("value1"))
		pair2 := node.NewPair([]byte("key2"), []byte("value2"))
		pair3 := node.NewPair([]byte("key3"), []byte("value3"))

		bufferPage := createLeafBufferPage(page.NewPageId(page.FileId(0), page.PageNumber(0)), []node.Pair{pair1, pair2, pair3}, nil)
		iterator := newIterator(bufferPage, 1) // 2番目のペアを指している

		// WHEN
		pair, ok := iterator.Get()

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte("key2"), pair.Key)
		assert.Equal(t, []byte("value2"), pair.Value)
	})
}

func TestAdvance(t *testing.T) {
	t.Run("現在のページ内に、まだ次の key-value ペアがある場合は何もしない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		dm, _ := initDiskManagerForIterator(t, tmpdir)
		bpm := bufferpool.NewBufferPoolManager(3)
		bpm.RegisterDiskManager(page.FileId(0), dm)

		pair1 := node.NewPair([]byte("key1"), []byte("value1"))
		pair2 := node.NewPair([]byte("key2"), []byte("value2"))
		pair3 := node.NewPair([]byte("key3"), []byte("value3"))

		bufferPage := createLeafBufferPage(page.NewPageId(page.FileId(0), page.PageNumber(0)), []node.Pair{pair1, pair2, pair3}, nil)
		iterator := newIterator(bufferPage, 0)

		// WHEN
		err := iterator.Advance(bpm)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(0)), iterator.bufferPage.PageId) // ページは変わらない
	})

	t.Run("現在のページ内に、次の key-value ペアがないが、次のページも存在しない場合は何もしない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		dm, _ := initDiskManagerForIterator(t, tmpdir)
		bpm := bufferpool.NewBufferPoolManager(3)
		bpm.RegisterDiskManager(page.FileId(0), dm)

		pair1 := node.NewPair([]byte("key1"), []byte("value1"))
		pair2 := node.NewPair([]byte("key2"), []byte("value2"))

		bufferPage := createLeafBufferPage(page.NewPageId(page.FileId(0), page.PageNumber(0)), []node.Pair{pair1, pair2}, nil)
		iterator := newIterator(bufferPage, 1) // 最後のペアを指している

		// WHEN
		err := iterator.Advance(bpm)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(0)), iterator.bufferPage.PageId) // ページは変わらない
	})

	t.Run("現在のページ内に次の key-value ペアがなく、次のページが存在する場合は、次のページに移動する (古いページの参照ビットがクリアされ、次のページの先頭にポインタが置かれる)", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		dm, _ := initDiskManagerForIterator(t, tmpdir)
		bpm := bufferpool.NewBufferPoolManager(3)
		bpm.RegisterDiskManager(page.FileId(0), dm)

		// 最初のページ
		pair1 := node.NewPair([]byte("key1"), []byte("value1"))
		pair2 := node.NewPair([]byte("key2"), []byte("value2"))
		nextPageId := page.NewPageId(page.FileId(0), page.PageNumber(1))
		bufferPage1 := createLeafBufferPage(page.NewPageId(page.FileId(0), page.PageNumber(0)), []node.Pair{pair1, pair2}, &nextPageId)

		// 次のページ
		pair3 := node.NewPair([]byte("key3"), []byte("value3"))
		pair4 := node.NewPair([]byte("key4"), []byte("value4"))
		bufferPage2 := createLeafBufferPage(page.NewPageId(page.FileId(0), page.PageNumber(1)), []node.Pair{pair3, pair4}, nil)

		// 次のページをディスクに書き込む
		err := dm.WritePageData(page.NewPageId(page.FileId(0), page.PageNumber(1)), bufferPage2.GetReadData())
		assert.NoError(t, err)

		// ページ1をバッファプールに追加
		addedPage1, err := bpm.AddPage(page.NewPageId(page.FileId(0), page.PageNumber(0)))
		assert.NoError(t, err)
		copy(addedPage1.GetWriteData(), bufferPage1.GetReadData())

		iterator := newIterator(*addedPage1, 1) // 最後のペアを指している

		// WHEN
		err = iterator.Advance(bpm)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(1)), iterator.bufferPage.PageId)
		assert.Equal(t, 0, iterator.bufferId) // 次のページの先頭
	})
}

func TestNext(t *testing.T) {
	t.Run("次の key-value ペアの情報が取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		dm, _ := initDiskManagerForIterator(t, tmpdir)
		bpm := bufferpool.NewBufferPoolManager(3)
		bpm.RegisterDiskManager(page.FileId(0), dm)

		pair1 := node.NewPair([]byte("key1"), []byte("value1"))
		pair2 := node.NewPair([]byte("key2"), []byte("value2"))
		pair3 := node.NewPair([]byte("key3"), []byte("value3"))

		bufferPage := createLeafBufferPage(page.NewPageId(page.FileId(0), page.PageNumber(0)), []node.Pair{pair1, pair2, pair3}, nil)
		iterator := newIterator(bufferPage, 0)
		assert.Equal(t, 0, iterator.bufferId) // 最初のペアを指している

		// WHEN
		pair, ok, err := iterator.Next(bpm)
		bufferId1 := iterator.bufferId
		pair2Result, ok2, err2 := iterator.Next(bpm)
		bufferId2 := iterator.bufferId

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte("key1"), pair.Key)
		assert.Equal(t, []byte("value1"), pair.Value)
		assert.Equal(t, 1, bufferId1) // 1回目のNext()後のbufferId
		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, []byte("key2"), pair2Result.Key)
		assert.Equal(t, []byte("value2"), pair2Result.Value)
		assert.Equal(t, 2, bufferId2) // 2回目のNext()後のbufferId
	})
}

// リーフノードを含む BufferPage を作成する
func createLeafBufferPage(pageId page.PageId, pairs []node.Pair, nextPageId *page.PageId) bufferpool.BufferPage {
	bufpool := bufferpool.NewBufferPage(pageId)

	leafNode := node.NewLeafNode(bufpool.GetWriteData())
	leafNode.Initialize()

	// ペアを挿入
	for i, pair := range pairs {
		if !leafNode.Insert(i, pair) {
			panic("failed to insert pair")
		}
	}

	// nextPageId を設定
	leafNode.SetNextPageId(nextPageId)

	return *bufpool
}

func initDiskManagerForIterator(t *testing.T, tmpdir string) (*disk.DiskManager, page.PageId) {
	path := filepath.Join(tmpdir, "iterator_test.db")
	dm, err := disk.NewDiskManager(page.FileId(0), path)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	pageId := dm.AllocatePage()
	return dm, pageId
}
