package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestDelete(t *testing.T) {
	t.Run("レコードを削除できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x20}, []byte{0xBB}))

		// WHEN
		err := bt.Delete(mtr, []byte{0x10})

		// THEN
		assert.NoError(t, err)
		_, _, err = bt.FindByKey(mtr, []byte{0x10})
		assert.ErrorIs(t, err, ErrKeyNotFound)
		record, _, err := bt.FindByKey(mtr, []byte{0x20})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0xBB}, record.NonKey())
	})

	t.Run("存在しないキーを削除すると ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		err := bt.Delete(mtr, []byte{0xFF})

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("空の B+Tree から削除すると ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		err := bt.Delete(mtr, []byte{0x10})

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("削除後にリーフマージが発生すると leafPageCount がデクリメントされる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x01}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x02}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x03}, nonKey))
		countBefore, _ := bt.LeafPageCount(mtr)
		assert.Equal(t, uint64(2), countBefore)

		// WHEN
		err := bt.Delete(mtr, []byte{0x03})

		// THEN
		assert.NoError(t, err)
		countAfter, _ := bt.LeafPageCount(mtr)
		assert.Equal(t, countBefore-1, countAfter)
	})

	t.Run("削除後にルート縮退が発生すると height がデクリメントされる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x01}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x02}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x03}, nonKey))
		heightBefore, _ := bt.Height(mtr)
		assert.Equal(t, uint64(2), heightBefore)

		// WHEN
		err := bt.Delete(mtr, []byte{0x03})

		// THEN
		assert.NoError(t, err)
		heightAfter, _ := bt.Height(mtr)
		assert.Equal(t, heightBefore-1, heightAfter)
	})

	t.Run("全レコードを削除できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x20}, []byte{0xBB}))

		// WHEN
		err1 := bt.Delete(mtr, []byte{0x10})
		err2 := bt.Delete(mtr, []byte{0x20})

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		iter, err := bt.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)
		_, ok, _ := iter.Get()
		assert.False(t, ok)
	})

	t.Run("ブランチノード経由で削除してもアンダーフローしない場合は isLeafMerged が false", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x01}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x02}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x03}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x04}, nonKey))
		height, _ := bt.Height(mtr)
		assert.Equal(t, uint64(2), height)
		countBefore, _ := bt.LeafPageCount(mtr)

		// WHEN
		err := bt.Delete(mtr, []byte{0x02})

		// THEN
		assert.NoError(t, err)
		countAfter, _ := bt.LeafPageCount(mtr)
		assert.Equal(t, countBefore, countAfter)
		_, _, err = bt.FindByKey(mtr, []byte{0x01})
		assert.NoError(t, err)
		_, _, err = bt.FindByKey(mtr, []byte{0x03})
		assert.NoError(t, err)
		_, _, err = bt.FindByKey(mtr, []byte{0x04})
		assert.NoError(t, err)
	})

	t.Run("削除後もアンダーフローしない場合はメタデータが変わらない", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x20}, []byte{0xBB}))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x30}, []byte{0xCC}))
		countBefore, _ := bt.LeafPageCount(mtr)
		heightBefore, _ := bt.Height(mtr)

		// WHEN
		err := bt.Delete(mtr, []byte{0x20})

		// THEN
		assert.NoError(t, err)
		countAfter, _ := bt.LeafPageCount(mtr)
		heightAfter, _ := bt.Height(mtr)
		assert.Equal(t, countBefore, countAfter)
		assert.Equal(t, heightBefore, heightAfter)
	})
}

func TestDeleteOptimistic(t *testing.T) {
	t.Run("高さ 1 ではアンダーフローしても needsPessimistic=false で削除が完了する", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		needsPessimistic, err := bt.deleteOptimistic(mtr, []byte{0x10})

		// THEN
		assert.NoError(t, err)
		assert.False(t, needsPessimistic)
		_, _, err = bt.FindByKey(mtr, []byte{0x10})
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("存在しないキーは ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		_, err := bt.deleteOptimistic(mtr, []byte{0xFF})

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("高さ 2 以上でアンダーフロー見込みなら needsPessimistic=true を返し削除しない", func(t *testing.T) {
		// GIVEN: 高さ 2 のツリーを作り、左リーフを 1 件だけ残してアンダーフロー寸前にする
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x01}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x02}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x03}, nonKey))
		height, _ := bt.Height(mtr)
		assert.Equal(t, uint64(2), height)

		// WHEN
		needsPessimistic, err := bt.deleteOptimistic(mtr, []byte{0x01})

		// THEN
		assert.NoError(t, err)
		assert.True(t, needsPessimistic)
		// 楽観モードで失敗したのでレコードは残っている
		_, _, err = bt.FindByKey(mtr, []byte{0x01})
		assert.NoError(t, err)
	})

	t.Run("完了後に Pin と Tree ラッチが残らない", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		_, err := bt.deleteOptimistic(mtr, []byte{0x10})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 0, mtr.PinnedCount())
		assert.Equal(t, 0, mtr.HeldLatchCount())
	})
}

func TestDeletePessimistic(t *testing.T) {
	t.Run("Tree SX ラッチを取得して削除できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		err := bt.deletePessimistic(mtr, []byte{0x10})

		// THEN
		assert.NoError(t, err)
		_, _, err = bt.FindByKey(mtr, []byte{0x10})
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("完了後に Pin と Tree ラッチが残らない", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		err := bt.deletePessimistic(mtr, []byte{0x10})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 0, mtr.PinnedCount())
		assert.Equal(t, 0, mtr.HeldLatchCount())
	})
}
