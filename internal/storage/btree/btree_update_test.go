package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestUpdate(t *testing.T) {
	t.Run("レコードの非キーを更新できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		err := bt.Update(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xBB}))

		// THEN
		assert.NoError(t, err)
		record, _, err := bt.FindByKey(mtr, []byte{0x10})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0xBB}, record.NonKey())
	})

	t.Run("存在しないキーを更新すると ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		err := bt.Update(mtr, NewRecord([]byte{}, []byte{0xFF}, []byte{0xBB}))

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("空の B+Tree で更新すると ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		err := bt.Update(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("更新後もキーは変わらない", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x20}, []byte{0xBB}))

		// WHEN
		err := bt.Update(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xFF}))

		// THEN
		assert.NoError(t, err)
		record, _, err := bt.FindByKey(mtr, []byte{0x10})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0x10}, record.Key())
		assert.Equal(t, []byte{0xFF}, record.NonKey())
		// 他のレコードに影響がないことを確認
		other, _, err := bt.FindByKey(mtr, []byte{0x20})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0xBB}, other.NonKey())
	})

	t.Run("ブランチノードを経由してリーフノードのレコードを更新できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x01}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x02}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x03}, nonKey))
		height, _ := bt.Height()
		assert.Equal(t, uint64(2), height)

		// WHEN
		newNonKey := make([]byte, 1500)
		newNonKey[0] = 0xFF
		err := bt.Update(mtr, NewRecord([]byte{}, []byte{0x01}, newNonKey))

		// THEN
		assert.NoError(t, err)
		record, _, err := bt.FindByKey(mtr, []byte{0x01})
		assert.NoError(t, err)
		assert.Equal(t, byte(0xFF), record.NonKey()[0])
	})

	t.Run("非キーのサイズが大きすぎて更新できない場合はエラーを返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x20}, nonKey))

		// WHEN (ページの空き容量を超える nonKey で更新)
		hugeNonKey := make([]byte, 3000)
		err := bt.Update(mtr, NewRecord([]byte{}, []byte{0x10}, hugeNonKey))

		// THEN
		assert.Error(t, err)
	})

	t.Run("同じレコードを複数回更新できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		_ = bt.Update(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xBB}))
		err := bt.Update(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xCC}))

		// THEN
		assert.NoError(t, err)
		record, _, err := bt.FindByKey(mtr, []byte{0x10})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0xCC}, record.NonKey())
	})
}

func TestUpdateOptimistic(t *testing.T) {
	t.Run("サイズが収まる場合は needsPessimistic=false で更新が完了する", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		needsPessimistic, err := bt.updateOptimistic(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xBB}))

		// THEN
		assert.NoError(t, err)
		assert.False(t, needsPessimistic)
		record, _, err := bt.FindByKey(mtr, []byte{0x10})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0xBB}, record.NonKey())
	})

	t.Run("サイズが収まらない場合は needsPessimistic=true を返し更新しない", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, nonKey))

		// WHEN
		hugeNonKey := make([]byte, 3000)
		needsPessimistic, err := bt.updateOptimistic(mtr, NewRecord([]byte{}, []byte{0x10}, hugeNonKey))

		// THEN
		assert.NoError(t, err)
		assert.True(t, needsPessimistic)
		record, _, err := bt.FindByKey(mtr, []byte{0x10})
		assert.NoError(t, err)
		assert.Equal(t, 1500, len(record.NonKey()))
	})

	t.Run("存在しないキーは ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		_, err := bt.updateOptimistic(mtr, NewRecord([]byte{}, []byte{0xFF}, []byte{0xBB}))

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("完了後に Pin と Tree ラッチが残らない", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		_, err := bt.updateOptimistic(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xBB}))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 0, mtr.PinnedCount())
		assert.Equal(t, 0, mtr.HeldLatchCount())
	})
}

func TestUpdatePessimistic(t *testing.T) {
	t.Run("Tree SX ラッチを取得して更新できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		err := bt.updatePessimistic(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xBB}))

		// THEN
		assert.NoError(t, err)
		record, _, err := bt.FindByKey(mtr, []byte{0x10})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0xBB}, record.NonKey())
	})

	t.Run("完了後に Pin と Tree ラッチが残らない", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		err := bt.updatePessimistic(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xBB}))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 0, mtr.PinnedCount())
		assert.Equal(t, 0, mtr.HeldLatchCount())
	})
}
