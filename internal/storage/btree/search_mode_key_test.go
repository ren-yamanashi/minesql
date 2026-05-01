package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestSearchModeKeySlotNum(t *testing.T) {
	t.Run("キーが存在する場合はそのスロット番号を返す", func(t *testing.T) {
		// GIVEN
		ln := newSearchModeKeyTestLeafNode()
		ln.Insert(0, node.NewRecord([]byte{0x01}, []byte{0x10}, []byte{}))
		ln.Insert(1, node.NewRecord([]byte{0x01}, []byte{0x20}, []byte{}))
		sm := SearchModeKey{Key: []byte{0x20}}

		// WHEN
		result := sm.slotNum(ln)

		// THEN
		assert.Equal(t, 1, result)
	})

	t.Run("キーが存在しない場合は挿入位置を返す", func(t *testing.T) {
		// GIVEN
		ln := newSearchModeKeyTestLeafNode()
		ln.Insert(0, node.NewRecord([]byte{0x01}, []byte{0x10}, []byte{}))
		ln.Insert(1, node.NewRecord([]byte{0x01}, []byte{0x30}, []byte{}))
		sm := SearchModeKey{Key: []byte{0x20}}

		// WHEN
		result := sm.slotNum(ln)

		// THEN
		assert.Equal(t, 1, result)
	})
}

func TestSearchModeKeyChildPageId(t *testing.T) {
	t.Run("キーに対応する子の PageId を返す", func(t *testing.T) {
		// GIVEN
		bn := newSearchModeKeyTestBranchNode()
		sm := SearchModeKey{Key: []byte{0x10}}

		// WHEN
		id, err := sm.childPageId(bn)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.NewPageId(0, 1), id)
	})

	t.Run("キーが全レコードより大きい場合は右の子の PageId を返す", func(t *testing.T) {
		// GIVEN
		bn := newSearchModeKeyTestBranchNode()
		sm := SearchModeKey{Key: []byte{0xFF}}

		// WHEN
		id, err := sm.childPageId(bn)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.NewPageId(0, 2), id)
	})
}

// newSearchModeKeyTestLeafNode はテスト用の初期化済み LeafNode を作成する
func newSearchModeKeyTestLeafNode() *node.LeafNode {
	data := make([]byte, 256)
	ln := node.NewLeafNode(data)
	ln.Initialize()
	return ln
}

// newSearchModeKeyTestBranchNode はテスト用の初期化済み BranchNode を作成する
func newSearchModeKeyTestBranchNode() *node.BranchNode {
	data := make([]byte, 256)
	bn := node.NewBranchNode(data)
	bn.Initialize([]byte{0x10}, page.NewPageId(0, 1), page.NewPageId(0, 2))
	return bn
}
