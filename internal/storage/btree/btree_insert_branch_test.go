package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestInsertBranchOverflow(t *testing.T) {
	t.Run("ブランチノードにオーバーフローレコードを挿入できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)
		branchNode := setupTestBranchNode(t, bp)

		// WHEN
		overflowKey, newPageId, err := bt.insertBranchOverflow(
			branchNode,
			1,
			[]byte{0x20},
			page.NewPageId(0, 10),
		)

		// THEN
		assert.NoError(t, err)
		assert.Nil(t, overflowKey)
		assert.True(t, newPageId.IsInvalid())
		assert.Equal(t, 2, branchNode.NumRecords())
	})

	t.Run("ブランチノードが満杯の場合は分割される", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)
		branchNode := setupTestBranchNode(t, bp)
		fillBranchNodeUntilFull(branchNode)

		// WHEN
		overflowKey, newPageId, err := bt.insertBranchOverflow(
			branchNode,
			branchNode.NumRecords(),
			[]byte{0xFF},
			page.NewPageId(0, 99),
		)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, overflowKey)
		assert.False(t, newPageId.IsInvalid())
	})
}

// setupTestBranchNode はテスト用の初期化済みブランチノードを作成する
func setupTestBranchNode(t *testing.T, bp *buffer.BufferPool) *node.BranchNode {
	t.Helper()
	pageId, err := bp.AllocatePageId(0)
	assert.NoError(t, err)
	_, err = bp.AddPage(pageId)
	assert.NoError(t, err)
	pg, err := bp.GetWritePage(pageId)
	assert.NoError(t, err)
	bn := node.NewBranchNode(pg)
	err = bn.Initialize([]byte{0x10}, page.NewPageId(0, 1), page.NewPageId(0, 2))
	assert.NoError(t, err)
	return bn
}

// fillBranchNodeUntilFull はブランチノードを Insert が失敗するまで埋める
func fillBranchNodeUntilFull(bn *node.BranchNode) {
	for i := range 1000 {
		key := []byte{byte(i/256 + 0x11), byte(i % 256)}
		record := node.NewRecord([]byte{}, key, page.NewPageId(0, page.PageNumber(i+10)).ToBytes())
		if !bn.Insert(bn.NumRecords(), record) {
			return
		}
	}
}
