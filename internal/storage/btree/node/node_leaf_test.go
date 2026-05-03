package node

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestLeafNodeSplitInsert(t *testing.T) {
	t.Run("挿入キーが先頭キーより大きい場合に分割できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		padding := make([]byte, 8)
		for i := range 150 {
			ln.Insert(i, NewRecord([]byte{0x01}, []byte{byte(i/256 + 1), byte(i % 256)}, padding))
		}
		newLeaf := newTestLeafNode()
		newRecord := NewRecord([]byte{0x01}, []byte{0xFF}, padding)

		// WHEN
		key, err := ln.SplitInsert(newLeaf, newRecord)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, key)
		assert.True(t, ln.NumRecords() > 0)
		assert.True(t, newLeaf.NumRecords() > 0)
	})

	t.Run("挿入キーが先頭キー以下の場合に分割できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		padding := make([]byte, 8)
		for i := range 150 {
			ln.Insert(i, NewRecord([]byte{0x01}, []byte{byte(i/256 + 1), byte(i % 256)}, padding))
		}
		newLeaf := newTestLeafNode()
		newRecord := NewRecord([]byte{0x01}, []byte{0x00}, padding)

		// WHEN
		key, err := ln.SplitInsert(newLeaf, newRecord)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, key)
		assert.True(t, ln.NumRecords() > 0)
		assert.True(t, newLeaf.NumRecords() > 0)
	})

	t.Run("分割後に古いノードの容量が不足するとエラーを返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		maxSize := ln.maxRecordSize()
		bigNonKey := make([]byte, maxSize-6)
		bigNonKey[0] = 0x01
		ln.Insert(0, NewRecord([]byte{0x01}, []byte{0x01}, bigNonKey))
		bigNonKey2 := make([]byte, maxSize-6)
		bigNonKey2[0] = 0x02
		ln.Insert(1, NewRecord([]byte{0x01}, []byte{0x02}, bigNonKey2))
		newLeaf := newTestLeafNode()
		bigNonKey3 := make([]byte, maxSize-6)
		bigNonKey3[0] = 0x03
		newRecord := NewRecord([]byte{0x01}, []byte{0x01, 0x01}, bigNonKey3)

		// WHEN
		key, err := ln.SplitInsert(newLeaf, newRecord)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, key)
	})
}

func TestLeafNodePrevPageId(t *testing.T) {
	t.Run("初期化後は InvalidPageId を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()

		// WHEN
		id := ln.PrevPageId()

		// THEN
		assert.Equal(t, page.InvalidPageId, id)
	})
}

func TestLeafNodeNextPageId(t *testing.T) {
	t.Run("初期化後は InvalidPageId を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()

		// WHEN
		id := ln.NextPageId()

		// THEN
		assert.Equal(t, page.InvalidPageId, id)
	})
}

func TestLeafNodeSetPrevPageId(t *testing.T) {
	t.Run("前のリーフノードのページ ID を設定できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		prevId := page.NewPageId(0, 5)

		// WHEN
		ln.SetPrevPageId(prevId)

		// THEN
		assert.Equal(t, prevId, ln.PrevPageId())
	})
}

func TestLeafNodeSetNextPageId(t *testing.T) {
	t.Run("次のリーフノードのページ ID を設定できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		nextId := page.NewPageId(0, 10)

		// WHEN
		ln.SetNextPageId(nextId)

		// THEN
		assert.Equal(t, nextId, ln.NextPageId())
	})
}

func TestLeafNodeTransferAllFrom(t *testing.T) {
	t.Run("全レコードを転送できる", func(t *testing.T) {
		// GIVEN
		src := newTestLeafNode()
		src.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		src.Insert(1, NewRecord([]byte{0x02}, []byte{0x20}, []byte{0xBB}))
		dest := newTestLeafNode()

		// WHEN
		ok := dest.TransferAllFrom(src)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 0, src.NumRecords())
		assert.Equal(t, 2, dest.NumRecords())
		assert.Equal(t, []byte{0x10}, dest.Record(0).Key())
		assert.Equal(t, []byte{0x20}, dest.Record(1).Key())
	})
}

// newTestLeafNode は初期化済みの LeafNode を作成する
func newTestLeafNode() *LeafNode {
	pg, err := page.NewPage(make([]byte, page.PageSize))
	if err != nil {
		panic(err)
	}
	ln := NewLeafNode(pg)
	ln.Initialize()
	return ln
}
