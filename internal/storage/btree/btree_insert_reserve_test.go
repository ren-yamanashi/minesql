package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReservePages(t *testing.T) {
	t.Run("要求数分の leaf / branch ページを事前確保する", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		reserved, err := bt.reservePages(mtr, 3)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, 1, len(reserved.leaf))
		assert.Equal(t, 3, len(reserved.branch))
		bt.releaseUnused(mtr, reserved)
		require.NoError(t, mtr.Commit())
	})

	t.Run("takeLeaf / takeBranch は取り出したページを返す", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)
		mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		reserved, err := bt.reservePages(mtr, 2)
		require.NoError(t, err)

		// WHEN
		leafId := reserved.takeLeaf()
		branchId1 := reserved.takeBranch()
		branchId2 := reserved.takeBranch()

		// THEN
		assert.False(t, leafId.IsInvalid())
		assert.False(t, branchId1.IsInvalid())
		assert.False(t, branchId2.IsInvalid())
		assert.Equal(t, 0, len(reserved.leaf))
		assert.Equal(t, 0, len(reserved.branch))
		require.NoError(t, mtr.Commit())
	})

	t.Run("takeLeaf は空の場合 panic する", func(t *testing.T) {
		// GIVEN
		r := &reservedPages{}

		// THEN
		assert.Panics(t, func() { r.takeLeaf() })
	})

	t.Run("takeBranch は空の場合 panic する", func(t *testing.T) {
		// GIVEN
		r := &reservedPages{}

		// THEN
		assert.Panics(t, func() { r.takeBranch() })
	})

	t.Run("releaseUnused は残りページを全て解放する", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)
		mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		reserved, err := bt.reservePages(mtr, 2)
		require.NoError(t, err)
		leafId := reserved.takeLeaf()
		_ = leafId
		branchIds := []page.Id{reserved.branch[0], reserved.branch[1]}

		// WHEN
		bt.releaseUnused(mtr, reserved)

		// THEN 解放後は 2 つの branch ページが free に戻る
		for _, id := range branchIds {
			isFree, err := fsp.IsPageFree(mtr, id)
			require.NoError(t, err)
			assert.True(t, isFree, "reserved branch page should be free after releaseUnused: %v", id)
		}
		require.NoError(t, mtr.Commit())
	})

	t.Run("悲観挿入で 1 リーフ分割した場合は未使用の branch ページが返却され LeafPageCount は 1 だけ増える", func(t *testing.T) {
		// GIVEN 1 リーフ分割だけ発生する状態を作る (height=1、リーフを満杯にする)
		bt := setupBtree(t)
		nonKey := make([]byte, 1500)
		for i := byte(0); i < 2; i++ {
			bt.mustInsert(string([]byte{i + 1}), string(nonKey))
		}
		beforeMtr := buffer.NewMtr(bt.bufferPool)
		leafCountBefore, err := bt.LeafPageCount(beforeMtr)
		require.NoError(t, err)
		beforeMtr.UnpinAll()

		// WHEN 分割を必ず発生させる 3 つ目を挿入
		bt.mustInsert(string([]byte{3}), string(nonKey))

		// THEN リーフ 1 枚だけ増える (事前確保した branch ページは返却される)
		afterMtr := buffer.NewMtr(bt.bufferPool)
		defer afterMtr.UnpinAll()
		leafCountAfter, err := bt.LeafPageCount(afterMtr)
		require.NoError(t, err)
		assert.Equal(t, leafCountBefore+1, leafCountAfter)
	})
}
