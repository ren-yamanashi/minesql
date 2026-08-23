package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestInsertBranchOverflow(t *testing.T) {
	t.Run("ブランチノードにオーバーフローレコードを挿入できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)
		branchNode := setupTestBranchNode(t, bp)

		// WHEN
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		reserved := reserveForTest(t, bt, mtr, 1)
		overflowKey, newPageId := bt.insertBranchOverflow(
			mtr,
			branchNode,
			1,
			[]byte{0x20},
			page.NewId(0, 10),
			reserved,
		)

		// THEN
		assert.Nil(t, overflowKey)
		assert.True(t, newPageId.IsInvalid())
		assert.Equal(t, 2, branchNode.numRecords())
	})

	t.Run("ブランチノードが満杯の場合は分割される", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)
		branchNode := setupTestBranchNode(t, bp)
		fillBranchNodeUntilFull(branchNode)

		// WHEN
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		reserved := reserveForTest(t, bt, mtr, 1)
		overflowKey, newPageId := bt.insertBranchOverflow(
			mtr,
			branchNode,
			branchNode.numRecords(),
			[]byte{0xFF},
			page.NewId(0, 99),
			reserved,
		)

		// THEN
		assert.NotNil(t, overflowKey)
		assert.False(t, newPageId.IsInvalid())
	})
}

// setupTestBranchNode はテスト用の初期化済みブランチノードを作成する
func setupTestBranchNode(t *testing.T, bp *buffer.Pool) *branchNode {
	t.Helper()
	allocMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
	pageId, err := fsp.AllocatePage(allocMtr, page.FileId(0))
	assert.NoError(t, err)
	assert.NoError(t, allocMtr.Commit())
	_, err = bp.AddPage(pageId)
	assert.NoError(t, err)
	pg, err := bp.Page(pageId)
	assert.NoError(t, err)
	bn := newBranchNode(pg)
	bn.initialize([]byte{0x10}, page.NewId(0, 1), page.NewId(0, 2))
	return bn
}

// fillBranchNodeUntilFull はブランチノードを Insert が失敗するまで埋める
func fillBranchNodeUntilFull(bn *branchNode) {
	for i := range 1000 {
		key := []byte{byte(i/256 + 0x11), byte(i % 256)}
		record := NewRecord([]byte{}, key, page.NewId(0, page.PageNumber(i+10)).Bytes())
		if !bn.insert(bn.numRecords(), record) {
			return
		}
	}
}
