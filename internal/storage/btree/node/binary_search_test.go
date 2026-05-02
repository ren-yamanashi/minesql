package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBinarySearch(t *testing.T) {
	t.Run("キーが見つかった場合はインデックスと true を返す", func(t *testing.T) {
		// GIVEN
		node := newMockNode([][]byte{{0x01}, {0x02}, {0x03}})

		// WHEN
		index, found := binarySearch(node, []byte{0x02})

		// THEN
		assert.Equal(t, 1, index)
		assert.True(t, found)
	})

	t.Run("先頭のキーが見つかる", func(t *testing.T) {
		// GIVEN
		node := newMockNode([][]byte{{0x01}, {0x02}, {0x03}})

		// WHEN
		index, found := binarySearch(node, []byte{0x01})

		// THEN
		assert.Equal(t, 0, index)
		assert.True(t, found)
	})

	t.Run("末尾のキーが見つかる", func(t *testing.T) {
		// GIVEN
		node := newMockNode([][]byte{{0x01}, {0x02}, {0x03}})

		// WHEN
		index, found := binarySearch(node, []byte{0x03})

		// THEN
		assert.Equal(t, 2, index)
		assert.True(t, found)
	})

	t.Run("キーが見つからない場合は挿入位置と false を返す", func(t *testing.T) {
		// GIVEN
		node := newMockNode([][]byte{{0x01}, {0x03}, {0x05}})

		// WHEN
		index, found := binarySearch(node, []byte{0x02})

		// THEN
		assert.Equal(t, 1, index)
		assert.False(t, found)
	})

	t.Run("全要素より小さいキーの場合は先頭位置を返す", func(t *testing.T) {
		// GIVEN
		node := newMockNode([][]byte{{0x02}, {0x03}})

		// WHEN
		index, found := binarySearch(node, []byte{0x01})

		// THEN
		assert.Equal(t, 0, index)
		assert.False(t, found)
	})

	t.Run("全要素より大きいキーの場合は末尾の次の位置を返す", func(t *testing.T) {
		// GIVEN
		node := newMockNode([][]byte{{0x01}, {0x02}})

		// WHEN
		index, found := binarySearch(node, []byte{0x03})

		// THEN
		assert.Equal(t, 2, index)
		assert.False(t, found)
	})

	t.Run("要素が 1 つでキーが一致する", func(t *testing.T) {
		// GIVEN
		node := newMockNode([][]byte{{0x01}})

		// WHEN
		index, found := binarySearch(node, []byte{0x01})

		// THEN
		assert.Equal(t, 0, index)
		assert.True(t, found)
	})

	t.Run("要素が 1 つでキーが一致しない", func(t *testing.T) {
		// GIVEN
		node := newMockNode([][]byte{{0x02}})

		// WHEN
		index, found := binarySearch(node, []byte{0x01})

		// THEN
		assert.Equal(t, 0, index)
		assert.False(t, found)
	})

	t.Run("空のノードの場合はインデックス 0 と false を返す", func(t *testing.T) {
		// GIVEN
		node := newMockNode([][]byte{})

		// WHEN
		index, found := binarySearch(node, []byte{0x01})

		// THEN
		assert.Equal(t, 0, index)
		assert.False(t, found)
	})
}

// mockNode は binarySearch のテスト用の Node 実装
type mockNode struct {
	records []Record
}

func newMockNode(keys [][]byte) *mockNode {
	records := make([]Record, len(keys))
	for i, key := range keys {
		records[i] = NewRecord([]byte{}, key, []byte{})
	}
	return &mockNode{records: records}
}

func (m *mockNode) NumRecords() int                    { return len(m.records) }
func (m *mockNode) Record(slotNum int) Record          { return m.records[slotNum] }
func (m *mockNode) Insert(_ int, _ Record) bool        { return false }
func (m *mockNode) Delete(_ int)                       {}
func (m *mockNode) CanTransferRecord(_ bool) bool      { return false }
func (m *mockNode) Body() []byte                       { return nil }
func (m *mockNode) SearchSlotNum(_ []byte) (int, bool) { return 0, false }
func (m *mockNode) IsHalfFull() bool                   { return false }
func (m *mockNode) maxRecordSize() int                 { return 0 }
