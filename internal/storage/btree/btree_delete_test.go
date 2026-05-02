package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/stretchr/testify/assert"
)

func TestDelete(t *testing.T) {
	t.Run("レコードを削除できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		bt.Insert(node.NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))
		bt.Insert(node.NewRecord([]byte{}, []byte{0x20}, []byte{0xBB}))

		// WHEN
		err := bt.Delete([]byte{0x10})

		// THEN
		assert.NoError(t, err)
		_, _, err = bt.FindByKey([]byte{0x10})
		assert.ErrorIs(t, err, ErrKeyNotFound)
		record, _, err := bt.FindByKey([]byte{0x20})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0xBB}, record.NonKey())
	})

	t.Run("存在しないキーを削除すると ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		bt.Insert(node.NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		err := bt.Delete([]byte{0xFF})

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("空の B+Tree から削除すると ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)

		// WHEN
		err := bt.Delete([]byte{0x10})

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("削除後にリーフマージが発生すると leafPageCount がデクリメントされる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		nonKey := make([]byte, 1500)
		bt.Insert(node.NewRecord([]byte{}, []byte{0x01}, nonKey))
		bt.Insert(node.NewRecord([]byte{}, []byte{0x02}, nonKey))
		bt.Insert(node.NewRecord([]byte{}, []byte{0x03}, nonKey))
		countBefore, _ := bt.LeafPageCount()
		assert.Equal(t, uint64(2), countBefore)

		// WHEN
		err := bt.Delete([]byte{0x03})

		// THEN
		assert.NoError(t, err)
		countAfter, _ := bt.LeafPageCount()
		assert.Equal(t, countBefore-1, countAfter)
	})

	t.Run("削除後にルート縮退が発生すると height がデクリメントされる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		nonKey := make([]byte, 1500)
		bt.Insert(node.NewRecord([]byte{}, []byte{0x01}, nonKey))
		bt.Insert(node.NewRecord([]byte{}, []byte{0x02}, nonKey))
		bt.Insert(node.NewRecord([]byte{}, []byte{0x03}, nonKey))
		heightBefore, _ := bt.Height()
		assert.Equal(t, uint64(2), heightBefore)

		// WHEN
		err := bt.Delete([]byte{0x03})

		// THEN
		assert.NoError(t, err)
		heightAfter, _ := bt.Height()
		assert.Equal(t, heightBefore-1, heightAfter)
	})

	t.Run("全レコードを削除できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		bt.Insert(node.NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))
		bt.Insert(node.NewRecord([]byte{}, []byte{0x20}, []byte{0xBB}))

		// WHEN
		err1 := bt.Delete([]byte{0x10})
		err2 := bt.Delete([]byte{0x20})

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		iter, err := bt.Search(SearchModeStart{})
		assert.NoError(t, err)
		_, ok, _ := iter.Get()
		assert.False(t, ok)
	})

	t.Run("削除後もアンダーフローしない場合はメタデータが変わらない", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		bt.Insert(node.NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))
		bt.Insert(node.NewRecord([]byte{}, []byte{0x20}, []byte{0xBB}))
		bt.Insert(node.NewRecord([]byte{}, []byte{0x30}, []byte{0xCC}))
		countBefore, _ := bt.LeafPageCount()
		heightBefore, _ := bt.Height()

		// WHEN
		err := bt.Delete([]byte{0x20})

		// THEN
		assert.NoError(t, err)
		countAfter, _ := bt.LeafPageCount()
		heightAfter, _ := bt.Height()
		assert.Equal(t, countBefore, countAfter)
		assert.Equal(t, heightBefore, heightAfter)
	})
}
