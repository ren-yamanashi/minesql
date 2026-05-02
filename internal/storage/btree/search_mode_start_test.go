package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestSearchModeStartSlotNum(t *testing.T) {
	t.Run("常に 0 を返す", func(t *testing.T) {
		// GIVEN
		ln := newSearchModeStartTestLeafNode()
		ln.Insert(0, node.NewRecord([]byte{0x01}, []byte{0x10}, []byte{}))
		sm := SearchModeStart{}

		// WHEN
		result := sm.slotNum(ln)

		// THEN
		assert.Equal(t, 0, result)
	})
}

func TestSearchModeStartChildPageId(t *testing.T) {
	t.Run("先頭の子の PageId を返す", func(t *testing.T) {
		// GIVEN
		bn := newSearchModeStartTestBranchNode()
		sm := SearchModeStart{}

		// WHEN
		id, err := sm.childPageId(bn)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.NewPageId(0, 1), id)
	})
}

// newSearchModeStartTestLeafNode はテスト用の初期化済み LeafNode を作成する
func newSearchModeStartTestLeafNode() *node.LeafNode {
	data := make([]byte, page.PageSize)
	pg, err := page.NewPage(data)
	if err != nil {
		panic(err)
	}
	ln := node.NewLeafNode(pg)
	ln.Initialize()
	return ln
}

// newSearchModeStartTestBranchNode はテスト用の初期化済み BranchNode を作成する
func newSearchModeStartTestBranchNode() *node.BranchNode {
	data := make([]byte, page.PageSize)
	pg, err := page.NewPage(data)
	if err != nil {
		panic(err)
	}
	bn := node.NewBranchNode(pg)
	_ = bn.Initialize([]byte{0x10}, page.NewPageId(0, 1), page.NewPageId(0, 2))
	return bn
}
