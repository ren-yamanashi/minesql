package btree

import (
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewIterator(t *testing.T) {
	t.Run("正常にイテレータを生成できる", func(t *testing.T) {
		// GIVEN
		var bufferPageMock bufferpool.BufferPage
		bufferId := 0

		// WHEN
		iterator := NewIterator(bufferPageMock, bufferId)

		// THEN
		assert.NotNil(t, iterator)
		assert.Equal(t, bufferPageMock, iterator.bufferPage)
		assert.Equal(t, bufferId, iterator.bufferId)
	})
}

func TestAdvance(t *testing.T) {
	t.Run("現在のページ内に、まだ次の key-value ペアがある場合は何もしない", func(t *testing.T) {
		// GIVEN
		dmSpy := NewDiskManagerSpy()
		bpm := bufferpool.NewBufferPoolManager(dmSpy, 3)

		pair1 := node.NewPair([]byte("key1"), []byte("value1"))
		pair2 := node.NewPair([]byte("key2"), []byte("value2"))
		pair3 := node.NewPair([]byte("key3"), []byte("value3"))

		bufferPage := createLeafBufferPage(disk.PageId(0), []node.Pair{pair1, pair2, pair3}, nil)
		iterator := NewIterator(bufferPage, 0)

		// WHEN
		err := iterator.Advance(bpm)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, disk.PageId(0), iterator.bufferPage.PageId) // ページは変わらない
	})

	t.Run("現在のページ内に、次の key-value ペアがないが、次のページも存在しない場合は何もしない", func(t *testing.T) {
		// GIVEN
		dmSpy := NewDiskManagerSpy()
		bpm := bufferpool.NewBufferPoolManager(dmSpy, 3)

		pair1 := node.NewPair([]byte("key1"), []byte("value1"))
		pair2 := node.NewPair([]byte("key2"), []byte("value2"))

		bufferPage := createLeafBufferPage(disk.PageId(0), []node.Pair{pair1, pair2}, nil)
		iterator := NewIterator(bufferPage, 1) // 最後のペアを指している

		// WHEN
		err := iterator.Advance(bpm)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, disk.PageId(0), iterator.bufferPage.PageId) // ページは変わらない
	})

	t.Run("現在のページ内に次の key-value ペアがなく、次のページが存在する場合は、次のページに移動する (古いページの参照ビットがクリアされ、次のページの先頭にポインタが置かれる)", func(t *testing.T) {
		// GIVEN
		dmSpy := NewDiskManagerSpy()
		bpm := bufferpool.NewBufferPoolManager(dmSpy, 3)

		// 最初のページ
		pair1 := node.NewPair([]byte("key1"), []byte("value1"))
		pair2 := node.NewPair([]byte("key2"), []byte("value2"))
		nextPageId := disk.PageId(1)
		bufferPage1 := createLeafBufferPage(disk.PageId(0), []node.Pair{pair1, pair2}, &nextPageId)

		// 次のページ
		pair3 := node.NewPair([]byte("key3"), []byte("value3"))
		pair4 := node.NewPair([]byte("key4"), []byte("value4"))
		bufferPage2 := createLeafBufferPage(disk.PageId(1), []node.Pair{pair3, pair4}, nil)

		// 次のページをディスクマネージャに登録
		dmSpy.AddPage(disk.PageId(1), bufferPage2.Page)

		// ページ1をバッファプールに追加
		addedPage1, err := bpm.AddPage(disk.PageId(0))
		assert.NoError(t, err)
		copy(addedPage1.Page[:], bufferPage1.Page[:])
		addedPage1.Referenced = true // 参照ビットをセット

		iterator := NewIterator(*addedPage1, 1) // 最後のペアを指している

		// WHEN
		err = iterator.Advance(bpm)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, disk.PageId(1), iterator.bufferPage.PageId)
		assert.Equal(t, 0, iterator.bufferId) // 次のページの先頭

		// 古いページ (PageId=0) の参照ビットがクリアされていることを確認
		oldBufferPage, ok := bpm.GetBufferPage(disk.PageId(0))
		assert.True(t, ok)
		assert.False(t, oldBufferPage.Referenced)
	})
}

func TestNext(t *testing.T) {
	t.Run("次の key-value ペアの情報が取得できる", func(t *testing.T) {
		// GIVEN
		dmSpy := NewDiskManagerSpy()
		bpm := bufferpool.NewBufferPoolManager(dmSpy, 3)

		pair1 := node.NewPair([]byte("key1"), []byte("value1"))
		pair2 := node.NewPair([]byte("key2"), []byte("value2"))
		pair3 := node.NewPair([]byte("key3"), []byte("value3"))

		bufferPage := createLeafBufferPage(disk.PageId(0), []node.Pair{pair1, pair2, pair3}, nil)
		iterator := NewIterator(bufferPage, 0)
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
func createLeafBufferPage(pageId disk.PageId, pairs []node.Pair, nextPageId *disk.PageId) bufferpool.BufferPage {
	page := &disk.Page{}
	_node := node.NewNode(page[:])
	_node.InitAsLeafNode()
	leafNode := node.NewLeafNode(_node.Body())
	leafNode.Initialize()

	// ペアを挿入
	for i, pair := range pairs {
		if !leafNode.Insert(i, pair) {
			panic("failed to insert pair")
		}
	}

	// nextPageId を設定
	leafNode.SetNextPageId(nextPageId)

	return bufferpool.BufferPage{
		PageId: pageId,
		Page:   page,
	}
}

type DiskManagerSpy struct {
	pages map[disk.PageId]*disk.Page
}

func NewDiskManagerSpy() *DiskManagerSpy {
	return &DiskManagerSpy{
		pages: make(map[disk.PageId]*disk.Page),
	}
}

func (spy *DiskManagerSpy) ReadPageData(id disk.PageId, data []byte) error {
	if page, ok := spy.pages[id]; ok {
		copy(data, page[:])
	}
	return nil
}

func (spy *DiskManagerSpy) WritePageData(id disk.PageId, data []byte) error {
	page := &disk.Page{}
	copy(page[:], data)
	spy.pages[id] = page
	return nil
}

func (spy *DiskManagerSpy) AllocatePage() disk.PageId {
	return disk.PageId(len(spy.pages))
}

func (spy *DiskManagerSpy) Sync() error {
	return nil
}

func (spy *DiskManagerSpy) AddPage(pageId disk.PageId, page *disk.Page) {
	spy.pages[pageId] = page
}
