package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/stretchr/testify/assert"
)

func TestUpdate(t *testing.T) {
	t.Run("レコードの非キーを更新できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		bt.Insert(node.NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		err := bt.Update(node.NewRecord([]byte{}, []byte{0x10}, []byte{0xBB}))

		// THEN
		assert.NoError(t, err)
		record, _, err := bt.FindByKey([]byte{0x10})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0xBB}, record.NonKey())
	})

	t.Run("存在しないキーを更新すると ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		bt.Insert(node.NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		err := bt.Update(node.NewRecord([]byte{}, []byte{0xFF}, []byte{0xBB}))

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("空の B+Tree で更新すると ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)

		// WHEN
		err := bt.Update(node.NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("更新後もキーは変わらない", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		bt.Insert(node.NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))
		bt.Insert(node.NewRecord([]byte{}, []byte{0x20}, []byte{0xBB}))

		// WHEN
		err := bt.Update(node.NewRecord([]byte{}, []byte{0x10}, []byte{0xFF}))

		// THEN
		assert.NoError(t, err)
		record, _, err := bt.FindByKey([]byte{0x10})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0x10}, record.Key())
		assert.Equal(t, []byte{0xFF}, record.NonKey())
		// 他のレコードに影響がないことを確認
		other, _, err := bt.FindByKey([]byte{0x20})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0xBB}, other.NonKey())
	})

	t.Run("ブランチノードを経由してリーフノードのレコードを更新できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		nonKey := make([]byte, 1500)
		bt.Insert(node.NewRecord([]byte{}, []byte{0x01}, nonKey))
		bt.Insert(node.NewRecord([]byte{}, []byte{0x02}, nonKey))
		bt.Insert(node.NewRecord([]byte{}, []byte{0x03}, nonKey))
		height, _ := bt.Height()
		assert.Equal(t, uint64(2), height)

		// WHEN
		newNonKey := make([]byte, 1500)
		newNonKey[0] = 0xFF
		err := bt.Update(node.NewRecord([]byte{}, []byte{0x01}, newNonKey))

		// THEN
		assert.NoError(t, err)
		record, _, err := bt.FindByKey([]byte{0x01})
		assert.NoError(t, err)
		assert.Equal(t, byte(0xFF), record.NonKey()[0])
	})

	t.Run("非キーのサイズが大きすぎて更新できない場合はエラーを返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		nonKey := make([]byte, 1500)
		bt.Insert(node.NewRecord([]byte{}, []byte{0x10}, nonKey))
		bt.Insert(node.NewRecord([]byte{}, []byte{0x20}, nonKey))

		// WHEN (ページの空き容量を超える nonKey で更新)
		hugeNonKey := make([]byte, 3000)
		err := bt.Update(node.NewRecord([]byte{}, []byte{0x10}, hugeNonKey))

		// THEN
		assert.Error(t, err)
	})

	t.Run("同じレコードを複数回更新できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		bt.Insert(node.NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		bt.Update(node.NewRecord([]byte{}, []byte{0x10}, []byte{0xBB}))
		err := bt.Update(node.NewRecord([]byte{}, []byte{0x10}, []byte{0xCC}))

		// THEN
		assert.NoError(t, err)
		record, _, err := bt.FindByKey([]byte{0x10})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0xCC}, record.NonKey())
	})
}
