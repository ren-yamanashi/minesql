package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestSearchModeStartSlotNum(t *testing.T) {
	t.Run("常に 0 を返す", func(t *testing.T) {
		// GIVEN
		ln := newSearchModeStartTestLeafNode()
		ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{}))
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
		assert.Equal(t, page.NewId(0, 1), id)
	})
}

// newSearchModeStartTestLeafNode はテスト用の初期化済み LeafNode を作成する
func newSearchModeStartTestLeafNode() *leafNode {
	data := make([]byte, page.Size)
	pg, err := page.NewPage(data)
	if err != nil {
		panic(err)
	}
	ln := newLeafNode(pg)
	ln.initialize()
	return ln
}

// newSearchModeStartTestBranchNode はテスト用の初期化済み BranchNode を作成する
func newSearchModeStartTestBranchNode() *branchNode {
	data := make([]byte, page.Size)
	pg, err := page.NewPage(data)
	if err != nil {
		panic(err)
	}
	bn := newBranchNode(pg)
	_ = bn.initialize([]byte{0x10}, page.NewId(0, 1), page.NewId(0, 2))
	return bn
}
