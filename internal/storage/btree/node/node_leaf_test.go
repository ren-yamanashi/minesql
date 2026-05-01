package node

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestLeafNodeInsert(t *testing.T) {
	t.Run("レコードを挿入できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(256)
		record := NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA})

		// WHEN
		ok := ln.Insert(0, record)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 1, ln.NumRecords())
	})

	t.Run("maxRecordSize を超えるレコードは挿入できない", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(64)
		maxSize := ln.maxRecordSize()
		largeData := make([]byte, maxSize) // ToBytes で 4 バイト追加されるため超過する

		// WHEN
		ok := ln.Insert(0, NewRecord([]byte{}, []byte{}, largeData))

		// THEN
		assert.False(t, ok)
		assert.Equal(t, 0, ln.NumRecords())
	})
}

func TestLeafNodeSplitInsert(t *testing.T) {
	t.Run("挿入キーが先頭キーより大きい場合に分割できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(128)
		padding := make([]byte, 8)
		for i := range 8 {
			ln.Insert(i, NewRecord([]byte{0x01}, []byte{byte(i + 1)}, padding))
		}
		newLeaf := newTestLeafNode(128)
		newRecord := NewRecord([]byte{0x01}, []byte{0x05, 0x01}, padding)

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
		ln := newTestLeafNode(128)
		padding := make([]byte, 8)
		for i := range 8 {
			ln.Insert(i, NewRecord([]byte{0x01}, []byte{byte(i + 1)}, padding))
		}
		newLeaf := newTestLeafNode(128)
		newRecord := NewRecord([]byte{0x01}, []byte{0x00}, padding)

		// WHEN
		key, err := ln.SplitInsert(newLeaf, newRecord)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, key)
		assert.True(t, ln.NumRecords() > 0)
		assert.True(t, newLeaf.NumRecords() > 0)
	})

	t.Run("転送先の容量が不足している場合はエラーを返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(128)
		padding := make([]byte, 8)
		for i := range 8 {
			ln.Insert(i, NewRecord([]byte{0x01}, []byte{byte(i + 1)}, padding))
		}
		newLeaf := newTestLeafNode(48)
		newRecord := NewRecord([]byte{0x01}, []byte{0x05, 0x01}, padding)

		// WHEN
		key, err := ln.SplitInsert(newLeaf, newRecord)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, key)
	})
}

func TestLeafNodeRemove(t *testing.T) {
	t.Run("レコードを削除できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(256)
		ln.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		ln.Remove(0)

		// THEN
		assert.Equal(t, 0, ln.NumRecords())
	})
}

func TestLeafNodeUpdate(t *testing.T) {
	t.Run("レコードを更新できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(256)
		ln.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		newRecord := NewRecord([]byte{0x02}, []byte{0x10}, []byte{0xBB, 0xCC})

		// WHEN
		ok := ln.Update(0, newRecord)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte{0x02}, ln.Record(0).Header())
		assert.Equal(t, []byte{0xBB, 0xCC}, ln.Record(0).NonKey())
	})
}

func TestLeafNodeNumRecords(t *testing.T) {
	t.Run("レコード数を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(256)
		ln.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{}))
		ln.Insert(1, NewRecord([]byte{0x01}, []byte{0x20}, []byte{}))

		// WHEN / THEN
		assert.Equal(t, 2, ln.NumRecords())
	})
}

func TestLeafNodeCanTransferRecord(t *testing.T) {
	t.Run("レコードが 1 つ以下の場合は false を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(256)
		ln.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))

		// WHEN / THEN
		assert.False(t, ln.CanTransferRecord(true))
		assert.False(t, ln.CanTransferRecord(false))
	})

	t.Run("転送後も半分以上埋まっている場合は true を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(128)
		// 小さいレコードを複数挿入して半分以上埋める
		for i := range 10 {
			ln.Insert(i, NewRecord([]byte{0x01}, []byte{byte(i)}, []byte{0xAA, 0xBB}))
		}

		// WHEN / THEN
		assert.True(t, ln.CanTransferRecord(true))
		assert.True(t, ln.CanTransferRecord(false))
	})
}

func TestLeafNodeRecordAt(t *testing.T) {
	t.Run("指定したスロット番号のレコードを取得できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(256)
		ln.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		ln.Insert(1, NewRecord([]byte{0x02}, []byte{0x20}, []byte{0xBB}))

		// WHEN
		r := ln.Record(1)

		// THEN
		assert.Equal(t, []byte{0x02}, r.Header())
		assert.Equal(t, []byte{0x20}, r.Key())
		assert.Equal(t, []byte{0xBB}, r.NonKey())
	})
}

func TestLeafNodeSearchSlotNum(t *testing.T) {
	t.Run("キーが見つかった場合はスロット番号と true を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(256)
		ln.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{}))
		ln.Insert(1, NewRecord([]byte{0x01}, []byte{0x20}, []byte{}))

		// WHEN
		slotNum, found := ln.SearchSlotNum([]byte{0x20})

		// THEN
		assert.Equal(t, 1, slotNum)
		assert.True(t, found)
	})

	t.Run("キーが見つからない場合は挿入位置と false を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(256)
		ln.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{}))
		ln.Insert(1, NewRecord([]byte{0x01}, []byte{0x30}, []byte{}))

		// WHEN
		slotNum, found := ln.SearchSlotNum([]byte{0x20})

		// THEN
		assert.Equal(t, 1, slotNum)
		assert.False(t, found)
	})
}

func TestLeafNodePrevPageId(t *testing.T) {
	t.Run("初期化後は InvalidPageId を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(256)

		// WHEN
		id := ln.PrevPageId()

		// THEN
		assert.Equal(t, page.InvalidPageId, id)
	})
}

func TestLeafNodeNextPageId(t *testing.T) {
	t.Run("初期化後は InvalidPageId を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(256)

		// WHEN
		id := ln.NextPageId()

		// THEN
		assert.Equal(t, page.InvalidPageId, id)
	})
}

func TestLeafNodeSetPrevPageId(t *testing.T) {
	t.Run("前のリーフノードのページ ID を設定できる", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(256)
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
		ln := newTestLeafNode(256)
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
		src := newTestLeafNode(256)
		src.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		src.Insert(1, NewRecord([]byte{0x02}, []byte{0x20}, []byte{0xBB}))
		dest := newTestLeafNode(256)

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

func TestLeafNodeIsHalfFull(t *testing.T) {
	t.Run("空の場合は false を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(256)

		// WHEN / THEN
		assert.False(t, ln.IsHalfFull())
	})

	t.Run("半分以上埋まっている場合は true を返す", func(t *testing.T) {
		// GIVEN
		ln := newTestLeafNode(128)
		for i := range 10 {
			ln.Insert(i, NewRecord([]byte{0x01}, []byte{byte(i)}, []byte{0xAA, 0xBB}))
		}

		// WHEN / THEN
		assert.True(t, ln.IsHalfFull())
	})
}

// newTestLeafNode は初期化済みの LeafNode を作成する
func newTestLeafNode(size int) *LeafNode {
	data := make([]byte, size)
	ln := NewLeafNode(data)
	ln.Initialize()
	return ln
}
