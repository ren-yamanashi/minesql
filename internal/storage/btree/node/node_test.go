package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// テスト用の Node モック
type mockNode struct {
	records []Record
}

func (m *mockNode) Insert(_ int, _ Record) bool   { return false }
func (m *mockNode) Delete(_ int)                  {}
func (m *mockNode) CanTransferRecord(_ bool) bool { return false }
func (m *mockNode) Body() []byte                  { return nil }
func (m *mockNode) NumRecords() int               { return len(m.records) }
func (m *mockNode) RecordAt(slotNum int) Record   { return m.records[slotNum] }
func (m *mockNode) SearchSlotNum(key []byte) (int, bool) {
	return binarySearch(m, key)
}
func (m *mockNode) IsHalfFull() bool   { return false }
func (m *mockNode) maxRecordSize() int { return 0 }

func newMockNode(keys ...[]byte) *mockNode {
	records := make([]Record, len(keys))
	for i, k := range keys {
		records[i] = NewRecord(nil, k, nil)
	}
	return &mockNode{records: records}
}

func TestGetNodeType(t *testing.T) {
	t.Run("リーフノードの場合", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 4096)
		copy(data[0:8], NODE_TYPE_LEAF)

		// WHEN
		nodeType := GetNodeType(data)

		// THEN
		assert.Equal(t, NODE_TYPE_LEAF, nodeType)
	})

	t.Run("ブランチノードの場合", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 4096)
		copy(data[0:8], NODE_TYPE_BRANCH)

		// WHEN
		nodeType := GetNodeType(data)

		// THEN
		assert.Equal(t, NODE_TYPE_BRANCH, nodeType)
	})
}

func TestBinarySearch(t *testing.T) {
	t.Run("正常に見つかる場合、要素のインデックスを返す", func(t *testing.T) {
		// GIVEN
		node := newMockNode([]byte{1}, []byte{3}, []byte{5}, []byte{7}, []byte{9})

		// WHEN
		index, found := binarySearch(node, []byte{5})

		// THEN
		assert.True(t, found)
		assert.Equal(t, 2, index)
	})

	t.Run("見つからない場合、挿入すべき位置のインデックスを返す", func(t *testing.T) {
		// GIVEN
		node := newMockNode([]byte{1}, []byte{3}, []byte{5}, []byte{7}, []byte{9})

		// WHEN
		index, found := binarySearch(node, []byte{6})

		// THEN
		assert.False(t, found)
		assert.Equal(t, 3, index)
	})

	t.Run("空のノードの場合", func(t *testing.T) {
		// GIVEN
		node := newMockNode()

		// WHEN
		index, found := binarySearch(node, []byte{1})

		// THEN
		assert.False(t, found)
		assert.Equal(t, 0, index)
	})
}
