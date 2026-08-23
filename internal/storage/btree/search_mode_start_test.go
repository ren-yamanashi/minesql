package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestSearchModeStartSlotNum(t *testing.T) {
	t.Run("常に 0 を返す", func(t *testing.T) {
		// GIVEN
		ln := newSearchModeStartTestLeafNode(t)
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
		bn := newSearchModeStartTestBranchNode(t)
		sm := SearchModeStart{}

		// WHEN
		id, err := sm.childPageId(bn)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.NewId(0, 1), id)
	})
}

// newSearchModeStartTestLeafNode はテスト用の初期化済み LeafNode を作成する
func newSearchModeStartTestLeafNode(t *testing.T) *leafNode {
	t.Helper()
	pool := buffer.NewPool(page.Size, newTestRedoBuffer(t), nil)
	bufPage, err := pool.AddPage(page.NewId(0, 0))
	if err != nil {
		panic(err)
	}
	ln := newLeafNode(bufPage)
	ln.initialize()
	return ln
}

// newSearchModeStartTestBranchNode はテスト用の初期化済み BranchNode を作成する
func newSearchModeStartTestBranchNode(t *testing.T) *branchNode {
	t.Helper()
	pool := buffer.NewPool(page.Size, newTestRedoBuffer(t), nil)
	bufPage, err := pool.AddPage(page.NewId(0, 0))
	if err != nil {
		panic(err)
	}
	bn := newBranchNode(bufPage)
	bn.initialize([]byte{0x10}, page.NewId(0, 1), page.NewId(0, 2))
	return bn
}
