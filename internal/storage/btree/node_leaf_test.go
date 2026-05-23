package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestLeafNodeInsert(t *testing.T) {
	t.Run("レコードを挿入できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		record := NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA})

		// WHEN
		ok := ln.insert(0, record)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 1, ln.numRecords())
	})

	t.Run("maxRecordSize を超えるレコードは挿入できない", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		maxSize := ln.maxRecordSize()
		largeData := make([]byte, maxSize) // ToBytes で 4 バイト追加されるため超過する

		// WHEN
		ok := ln.insert(0, NewRecord([]byte{}, []byte{}, largeData))

		// THEN
		assert.False(t, ok)
		assert.Equal(t, 0, ln.numRecords())
	})
}

func TestLeafNodeSplitInsert(t *testing.T) {
	t.Run("挿入キーが先頭キーより大きい場合に分割できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		padding := make([]byte, 8)
		for i := range 150 {
			ln.insert(i, NewRecord([]byte{0x01}, []byte{byte(i/256 + 1), byte(i % 256)}, padding))
		}
		newLeaf := newTestLeafNode()
		newRecord := NewRecord([]byte{0x01}, []byte{0xFF}, padding)

		// WHEN
		key, err := ln.splitInsert(newLeaf, newRecord)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, key)
		assert.True(t, ln.numRecords() > 0)
		assert.True(t, newLeaf.numRecords() > 0)
	})

	t.Run("挿入キーが先頭キー以下の場合に分割できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		padding := make([]byte, 8)
		for i := range 150 {
			ln.insert(i, NewRecord([]byte{0x01}, []byte{byte(i/256 + 1), byte(i % 256)}, padding))
		}
		newLeaf := newTestLeafNode()
		newRecord := NewRecord([]byte{0x01}, []byte{0x00}, padding)

		// WHEN
		key, err := ln.splitInsert(newLeaf, newRecord)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, key)
		assert.True(t, ln.numRecords() > 0)
		assert.True(t, newLeaf.numRecords() > 0)
	})

	t.Run("分割後に古いノードの容量が不足するとエラーを返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		maxSize := ln.maxRecordSize()
		bigNonKey := make([]byte, maxSize-6)
		bigNonKey[0] = 0x01
		ln.insert(0, NewRecord([]byte{0x01}, []byte{0x01}, bigNonKey))
		bigNonKey2 := make([]byte, maxSize-6)
		bigNonKey2[0] = 0x02
		ln.insert(1, NewRecord([]byte{0x01}, []byte{0x02}, bigNonKey2))
		newLeaf := newTestLeafNode()
		bigNonKey3 := make([]byte, maxSize-6)
		bigNonKey3[0] = 0x03
		newRecord := NewRecord([]byte{0x01}, []byte{0x01, 0x01}, bigNonKey3)

		// WHEN
		key, err := ln.splitInsert(newLeaf, newRecord)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, key)
	})
}

func TestLeafNodeDelete(t *testing.T) {
	t.Run("レコードを削除できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		ln.delete(0)

		// THEN
		assert.Equal(t, 0, ln.numRecords())
	})
}

func TestLeafNodeUpdate(t *testing.T) {
	t.Run("レコードを更新できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		newRecord := NewRecord([]byte{0x02}, []byte{0x10}, []byte{0xBB, 0xCC})

		// WHEN
		ok := ln.update(0, newRecord)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte{0x02}, ln.record(0).Header())
		assert.Equal(t, []byte{0xBB, 0xCC}, ln.record(0).NonKey())
	})
}

func TestLeafNodeNumRecords(t *testing.T) {
	t.Run("レコード数を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{}))
		ln.insert(1, NewRecord([]byte{0x01}, []byte{0x20}, []byte{}))

		// WHEN / THEN
		assert.Equal(t, 2, ln.numRecords())
	})
}

func TestLeafNodeCanTransferRecord(t *testing.T) {
	t.Run("レコードが 1 つ以下の場合は false を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))

		// WHEN / THEN
		assert.False(t, ln.canTransferRecord(true))
		assert.False(t, ln.canTransferRecord(false))
	})

	t.Run("転送後も半分以上埋まっている場合は true を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		padding := make([]byte, 200)
		for i := range 15 {
			ln.insert(i, NewRecord([]byte{0x01}, []byte{byte(i)}, padding))
		}

		// WHEN / THEN
		assert.True(t, ln.canTransferRecord(true))
		assert.True(t, ln.canTransferRecord(false))
	})
}

func TestLeafNodeRecordAt(t *testing.T) {
	t.Run("指定したスロット番号のレコードを取得できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		ln.insert(1, NewRecord([]byte{0x02}, []byte{0x20}, []byte{0xBB}))

		// WHEN
		r := ln.record(1)

		// THEN
		assert.Equal(t, []byte{0x02}, r.Header())
		assert.Equal(t, []byte{0x20}, r.Key())
		assert.Equal(t, []byte{0xBB}, r.NonKey())
	})
}

func TestLeafNodeSearchSlotNum(t *testing.T) {
	t.Run("キーが見つかった場合はスロット番号と true を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{}))
		ln.insert(1, NewRecord([]byte{0x01}, []byte{0x20}, []byte{}))

		// WHEN
		slotNum, found := ln.searchSlotNum([]byte{0x20})

		// THEN
		assert.Equal(t, 1, slotNum)
		assert.True(t, found)
	})

	t.Run("キーが見つからない場合は挿入位置と false を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{}))
		ln.insert(1, NewRecord([]byte{0x01}, []byte{0x30}, []byte{}))

		// WHEN
		slotNum, found := ln.searchSlotNum([]byte{0x20})

		// THEN
		assert.Equal(t, 1, slotNum)
		assert.False(t, found)
	})
}

func TestLeafNodePrevPageId(t *testing.T) {
	t.Run("初期化後は InvalidPageId を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()

		// WHEN
		id := ln.prevPageId()

		// THEN
		assert.Equal(t, page.InvalidId, id)
	})
}

func TestLeafNodeNextPageId(t *testing.T) {
	t.Run("初期化後は InvalidPageId を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()

		// WHEN
		id := ln.nextPageId()

		// THEN
		assert.Equal(t, page.InvalidId, id)
	})
}

func TestLeafNodeSetPrevPageId(t *testing.T) {
	t.Run("前のリーフノードのページ ID を設定できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		prevId := page.NewId(0, 5)

		// WHEN
		ln.setPrevPageId(prevId)

		// THEN
		assert.Equal(t, prevId, ln.prevPageId())
	})
}

func TestLeafNodeSetNextPageId(t *testing.T) {
	t.Run("次のリーフノードのページ ID を設定できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		nextId := page.NewId(0, 10)

		// WHEN
		ln.setNextPageId(nextId)

		// THEN
		assert.Equal(t, nextId, ln.nextPageId())
	})
}

func TestLeafNodeTransferAllFrom(t *testing.T) {
	t.Run("全レコードを転送できる", func(t *testing.T) {
		// GIVEN
		src := newTestLeafNode()
		src.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		src.insert(1, NewRecord([]byte{0x02}, []byte{0x20}, []byte{0xBB}))
		dest := newTestLeafNode()

		// WHEN
		ok := dest.transferAllFrom(src)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 0, src.numRecords())
		assert.Equal(t, 2, dest.numRecords())
		assert.Equal(t, []byte{0x10}, dest.record(0).Key())
		assert.Equal(t, []byte{0x20}, dest.record(1).Key())
	})
}

func TestLeafNodeIsHalfFull(t *testing.T) {
	t.Run("空の場合は false を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()

		// WHEN / THEN
		assert.False(t, ln.isHalfFull())
	})

	t.Run("半分以上埋まっている場合は true を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode()
		padding := make([]byte, 200)
		for i := range 15 {
			ln.insert(i, NewRecord([]byte{0x01}, []byte{byte(i)}, padding))
		}

		// WHEN / THEN
		assert.True(t, ln.isHalfFull())
	})
}

// newTestLeafNode は初期化済みの LeafNode を作成する
func newTestLeafNode() *leafNode {
	pg, err := page.NewPage(make([]byte, page.PageSize))
	if err != nil {
		panic(err)
	}
	ln := newLeafNode(pg)
	ln.initialize()
	return ln
}
