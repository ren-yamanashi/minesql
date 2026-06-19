package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	t.Run("レコードを挿入できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		err := bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// THEN
		assert.NoError(t, err)
		record, _, err := bt.FindByKey(mtr, []byte{0x10})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0xAA}, record.NonKey())
	})

	t.Run("複数レコードをソート順に挿入できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x30}, []byte{0xCC}))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x20}, []byte{0xBB}))

		// THEN
		iter, err := bt.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)
		r1, ok, _ := iter.Next()
		assert.True(t, ok)
		assert.Equal(t, []byte{0x10}, r1.Key())
		r2, ok, _ := iter.Next()
		assert.True(t, ok)
		assert.Equal(t, []byte{0x20}, r2.Key())
		r3, ok, _ := iter.Next()
		assert.True(t, ok)
		assert.Equal(t, []byte{0x30}, r3.Key())
	})

	t.Run("重複キーを挿入すると ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		err := bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xBB}))

		// THEN
		assert.ErrorIs(t, err, ErrDuplicateKey)
	})

	t.Run("リーフノードの分割が発生すると leafPageCount がインクリメントされる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x01}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x02}, nonKey))
		countBefore, _ := bt.LeafPageCount()

		// WHEN
		err := bt.Insert(mtr, NewRecord([]byte{}, []byte{0x03}, nonKey))

		// THEN
		assert.NoError(t, err)
		countAfter, _ := bt.LeafPageCount()
		assert.Equal(t, countBefore+1, countAfter)
	})

	t.Run("ルートノードの分割が発生すると height がインクリメントされる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x01}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x02}, nonKey))
		heightBefore, _ := bt.Height()
		assert.Equal(t, uint64(1), heightBefore)

		// WHEN
		err := bt.Insert(mtr, NewRecord([]byte{}, []byte{0x03}, nonKey))

		// THEN
		assert.NoError(t, err)
		heightAfter, _ := bt.Height()
		assert.Equal(t, heightBefore+1, heightAfter)
	})

	t.Run("分割後も全レコードを検索できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)

		// WHEN
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x01}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x02}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x03}, nonKey))

		// THEN
		for _, key := range [][]byte{{0x01}, {0x02}, {0x03}} {
			_, _, err := bt.FindByKey(mtr, key)
			assert.NoError(t, err)
		}
	})

	t.Run("境界キーと同じキーを挿入すると正しい子ノードで重複検出される", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x01}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x02}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x03}, nonKey))
		height, _ := bt.Height()
		assert.Equal(t, uint64(2), height)

		// WHEN
		err := bt.Insert(mtr, NewRecord([]byte{}, []byte{0x02}, nonKey))

		// THEN
		assert.ErrorIs(t, err, ErrDuplicateKey)
	})

	t.Run("分割が発生しない場合はメタデータが変わらない", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))
		countBefore, _ := bt.LeafPageCount()
		heightBefore, _ := bt.Height()

		// WHEN
		err := bt.Insert(mtr, NewRecord([]byte{}, []byte{0x20}, []byte{0xBB}))

		// THEN
		assert.NoError(t, err)
		countAfter, _ := bt.LeafPageCount()
		heightAfter, _ := bt.Height()
		assert.Equal(t, countBefore, countAfter)
		assert.Equal(t, heightBefore, heightAfter)
	})
}

func TestInsertOptimistic(t *testing.T) {
	t.Run("分割不要なら needsPessimistic=false で挿入が完了する", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		needsPessimistic, err := bt.insertOptimistic(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// THEN
		assert.NoError(t, err)
		assert.False(t, needsPessimistic)
		record, _, err := bt.FindByKey(mtr, []byte{0x10})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0xAA}, record.NonKey())
	})

	t.Run("リーフが満杯なら needsPessimistic=true を返し挿入しない", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)
		_, err := bt.insertOptimistic(mtr, NewRecord([]byte{}, []byte{0x01}, nonKey))
		assert.NoError(t, err)
		_, err = bt.insertOptimistic(mtr, NewRecord([]byte{}, []byte{0x02}, nonKey))
		assert.NoError(t, err)

		// WHEN
		needsPessimistic, err := bt.insertOptimistic(mtr, NewRecord([]byte{}, []byte{0x03}, nonKey))

		// THEN
		assert.NoError(t, err)
		assert.True(t, needsPessimistic)
		// 楽観モードで失敗したので 0x03 は未挿入
		_, _, err = bt.FindByKey(mtr, []byte{0x03})
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("完了後に Pin と Tree ラッチが残らない", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		_, err := bt.insertOptimistic(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 0, mtr.PinnedCount())
		assert.Equal(t, 0, mtr.HeldLatchCount())
	})

	t.Run("重複キーは ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_, err := bt.insertOptimistic(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))
		assert.NoError(t, err)

		// WHEN
		_, err = bt.insertOptimistic(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xBB}))

		// THEN
		assert.ErrorIs(t, err, ErrDuplicateKey)
	})
}

func TestInsertPessimistic(t *testing.T) {
	t.Run("Tree SX ラッチを取得し分割込みで挿入できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)
		_ = bt.insertPessimistic(mtr, NewRecord([]byte{}, []byte{0x01}, nonKey))
		_ = bt.insertPessimistic(mtr, NewRecord([]byte{}, []byte{0x02}, nonKey))
		countBefore, _ := bt.LeafPageCount()

		// WHEN
		err := bt.insertPessimistic(mtr, NewRecord([]byte{}, []byte{0x03}, nonKey))

		// THEN
		assert.NoError(t, err)
		countAfter, _ := bt.LeafPageCount()
		assert.Equal(t, countBefore+1, countAfter)
	})

	t.Run("完了後に Pin と Tree ラッチが残らない", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		err := bt.insertPessimistic(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 0, mtr.PinnedCount())
		assert.Equal(t, 0, mtr.HeldLatchCount())
	})
}
