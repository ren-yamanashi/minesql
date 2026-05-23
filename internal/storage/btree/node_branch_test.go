package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestBranchNodeInitialize(t *testing.T) {
	t.Run("初期化後にレコードが 1 つ挿入され右の子が設定される", func(t *testing.T) {
		// GIVEN
		bn := newUninitializedBranchNode()
		leftChild := page.NewId(0, 1)
		rightChild := page.NewId(0, 2)

		// WHEN
		err := bn.initialize([]byte{0x10}, leftChild, rightChild)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, bn.numRecords())
		assert.Equal(t, []byte{0x10}, bn.record(0).Key())
		assert.Equal(t, rightChild, bn.rightChildPageId())
	})
}

func TestBranchNodeInsert(t *testing.T) {
	t.Run("レコードを挿入できる", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode()
		record := newBranchRecord([]byte{0x20}, page.NewId(0, 10))

		// WHEN
		ok := bn.insert(1, record)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 2, bn.numRecords())
	})

	t.Run("maxRecordSize を超えるレコードは挿入できない", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode()
		maxSize := bn.maxRecordSize()
		largeKey := make([]byte, maxSize)

		// WHEN
		ok := bn.insert(1, NewRecord([]byte{}, largeKey, page.NewId(0, 1).ToBytes()))

		// THEN
		assert.False(t, ok)
	})
}

func TestBranchNodeSplitInsert(t *testing.T) {
	t.Run("挿入キーが先頭キーより大きい場合に分割できる", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode()
		for i := range 150 {
			bn.insert(bn.numRecords(), newBranchRecord([]byte{byte(i/256 + 0x11), byte(i % 256)}, page.NewId(0, page.PageNumber(i+10))))
		}
		newBranch := newUninitializedBranchNode()
		newRecord := newBranchRecord([]byte{0xFF}, page.NewId(0, 99))

		// WHEN
		key, err := bn.splitInsert(newBranch, newRecord)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, key)
		assert.True(t, bn.numRecords() > 0)
		assert.True(t, newBranch.numRecords() > 0)
	})

	t.Run("挿入キーが先頭キー以下の場合に分割できる", func(t *testing.T) {
		// GIVEN
		bn := newUninitializedBranchNode()
		_ = bn.initialize([]byte{0x10}, page.NewId(0, 1), page.NewId(0, 2))
		for i := range 150 {
			bn.insert(bn.numRecords(), newBranchRecord([]byte{byte(i/256 + 0x11), byte(i % 256)}, page.NewId(0, page.PageNumber(i+10))))
		}
		newBranch := newUninitializedBranchNode()
		newRecord := newBranchRecord([]byte{0x01}, page.NewId(0, 99))

		// WHEN
		key, err := bn.splitInsert(newBranch, newRecord)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, key)
		assert.True(t, bn.numRecords() > 0)
		assert.True(t, newBranch.numRecords() > 0)
	})

	t.Run("分割後に古いノードの容量が不足するとエラーを返す", func(t *testing.T) {
		// GIVEN
		bn := newUninitializedBranchNode()
		maxSize := bn.maxRecordSize()
		bigKey := make([]byte, maxSize-12)
		bigKey[0] = 0x01
		_ = bn.initialize(bigKey, page.NewId(0, 1), page.NewId(0, 2))
		bigKey2 := make([]byte, maxSize-12)
		bigKey2[0] = 0x02
		bn.insert(bn.numRecords(), newBranchRecord(bigKey2, page.NewId(0, 10)))
		newBranch := newUninitializedBranchNode()
		bigKey3 := make([]byte, maxSize-12)
		bigKey3[0] = 0x01
		bigKey3[1] = 0x01
		newRecord := newBranchRecord(bigKey3, page.NewId(0, 99))

		// WHEN
		key, err := bn.splitInsert(newBranch, newRecord)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, key)
	})

	t.Run("挿入キーが大きい場合に転送先の容量が不足するとエラーを返す", func(t *testing.T) {
		// GIVEN
		bn := newUninitializedBranchNode()
		maxSize := bn.maxRecordSize()
		bigKey := make([]byte, maxSize-12)
		bigKey[0] = 0x01
		_ = bn.initialize(bigKey, page.NewId(0, 1), page.NewId(0, 2))
		bigKey2 := make([]byte, maxSize-12)
		bigKey2[0] = 0x02
		bn.insert(bn.numRecords(), newBranchRecord(bigKey2, page.NewId(0, 10)))
		newBranch := newUninitializedBranchNode()
		bigKey3 := make([]byte, maxSize-12)
		bigKey3[0] = 0x03
		newRecord := newBranchRecord(bigKey3, page.NewId(0, 99))

		// WHEN
		key, err := bn.splitInsert(newBranch, newRecord)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, key)
	})

	t.Run("挿入キーが小さく転送先に Insert できない場合はエラーを返す", func(t *testing.T) {
		// GIVEN
		bn := newUninitializedBranchNode()
		maxSize := bn.maxRecordSize()
		bigKey := make([]byte, maxSize-12)
		bigKey[0] = 0x10
		_ = bn.initialize(bigKey, page.NewId(0, 1), page.NewId(0, 2))
		bigKey2 := make([]byte, maxSize-12)
		bigKey2[0] = 0x20
		bn.insert(bn.numRecords(), newBranchRecord(bigKey2, page.NewId(0, 10)))
		newBranch := newUninitializedBranchNode()
		bigKey3 := make([]byte, maxSize-12)
		bigKey3[0] = 0x01
		newRecord := newBranchRecord(bigKey3, page.NewId(0, 99))

		// WHEN
		key, err := bn.splitInsert(newBranch, newRecord)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, key)
	})

	t.Run("挿入キーが小さい場合に転送先の容量が不足するとエラーを返す", func(t *testing.T) {
		// GIVEN
		bn := newUninitializedBranchNode()
		maxSize := bn.maxRecordSize()
		bigKey := make([]byte, maxSize-12)
		bigKey[0] = 0x10
		_ = bn.initialize(bigKey, page.NewId(0, 1), page.NewId(0, 2))
		bigKey2 := make([]byte, maxSize-12)
		bigKey2[0] = 0x20
		bn.insert(bn.numRecords(), newBranchRecord(bigKey2, page.NewId(0, 10)))
		newBranch := newUninitializedBranchNode()
		bigKey3 := make([]byte, maxSize-12)
		bigKey3[0] = 0x05
		newRecord := newBranchRecord(bigKey3, page.NewId(0, 99))

		// WHEN
		key, err := bn.splitInsert(newBranch, newRecord)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, key)
	})
}

func TestBranchNodeDelete(t *testing.T) {
	t.Run("レコードを削除できる", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode()
		bn.insert(1, newBranchRecord([]byte{0x20}, page.NewId(0, 10)))

		// WHEN
		bn.delete(1)

		// THEN
		assert.Equal(t, 1, bn.numRecords())
	})
}

func TestBranchNodeUpdate(t *testing.T) {
	t.Run("レコードを更新できる", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode()
		newRecord := newBranchRecord([]byte{0xFF}, page.NewId(0, 99))

		// WHEN
		ok := bn.update(0, newRecord)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte{0xFF}, bn.record(0).Key())
	})
}

func TestBranchNodeNumRecords(t *testing.T) {
	t.Run("レコード数を返す", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode()
		bn.insert(1, newBranchRecord([]byte{0x20}, page.NewId(0, 10)))

		// WHEN / THEN
		assert.Equal(t, 2, bn.numRecords())
	})
}

func TestBranchNodeCanTransferRecord(t *testing.T) {
	t.Run("レコードが 1 つ以下の場合は false を返す", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode()

		// WHEN / THEN
		assert.False(t, bn.canTransferRecord(true))
		assert.False(t, bn.canTransferRecord(false))
	})

	t.Run("転送後も半分以上埋まっている場合は true を返す", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode()
		for i := range 100 {
			key := make([]byte, 20)
			key[0] = byte(i/256 + 0x11)
			key[1] = byte(i % 256)
			bn.insert(bn.numRecords(), newBranchRecord(key, page.NewId(0, page.PageNumber(i+10))))
		}

		// WHEN / THEN
		assert.True(t, bn.canTransferRecord(true))
		assert.True(t, bn.canTransferRecord(false))
	})
}

func TestBranchNodeRecord(t *testing.T) {
	t.Run("指定したスロット番号のレコードを取得できる", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode()
		bn.insert(1, newBranchRecord([]byte{0x20}, page.NewId(0, 10)))

		// WHEN
		r := bn.record(1)

		// THEN
		assert.Equal(t, []byte{0x20}, r.Key())
	})
}

func TestBranchNodeSearchSlotNum(t *testing.T) {
	t.Run("キーが見つかった場合はスロット番号と true を返す", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode()
		bn.insert(1, newBranchRecord([]byte{0x20}, page.NewId(0, 10)))

		// WHEN
		slotNum, found := bn.searchSlotNum([]byte{0x20})

		// THEN
		assert.Equal(t, 1, slotNum)
		assert.True(t, found)
	})

	t.Run("キーが見つからない場合は挿入位置と false を返す", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode()
		bn.insert(1, newBranchRecord([]byte{0x30}, page.NewId(0, 10)))

		// WHEN
		slotNum, found := bn.searchSlotNum([]byte{0x20})

		// THEN
		assert.Equal(t, 1, slotNum)
		assert.False(t, found)
	})
}

func TestBranchNodeChildPageId(t *testing.T) {
	t.Run("通常のスロット番号の場合はレコードの NonKey から PageId を返す", func(t *testing.T) {
		// GIVEN
		childId := page.NewId(0, 10)
		bn := newTestBranchNode()
		bn.insert(1, newBranchRecord([]byte{0x20}, childId))

		// WHEN
		id, err := bn.childPageId(1)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, childId, id)
	})

	t.Run("スロット番号がレコード数と同じ場合は右の子の PageId を返す", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode()

		// WHEN
		id, err := bn.childPageId(bn.numRecords())

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, bn.rightChildPageId(), id)
	})
}

func TestBranchNodeRightChildPageId(t *testing.T) {
	t.Run("Initialize で設定した右の子の PageId を返す", func(t *testing.T) {
		// GIVEN
		rightChild := page.NewId(0, 2)
		bn := newUninitializedBranchNode()
		_ = bn.initialize([]byte{0x10}, page.NewId(0, 1), rightChild)

		// WHEN
		id := bn.rightChildPageId()

		// THEN
		assert.Equal(t, rightChild, id)
	})
}

func TestBranchNodeSetRightChildPageId(t *testing.T) {
	t.Run("右の子の PageId を更新できる", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode()
		newId := page.NewId(1, 99)

		// WHEN
		bn.setRightChildPageId(newId)

		// THEN
		assert.Equal(t, newId, bn.rightChildPageId())
	})
}

func TestBranchNodeTransferAllFrom(t *testing.T) {
	t.Run("全レコードを転送できる", func(t *testing.T) {
		// GIVEN
		src := newTestBranchNode()
		src.insert(1, newBranchRecord([]byte{0x20}, page.NewId(0, 10)))
		dest := newTestBranchNode()

		// WHEN
		ok := dest.transferAllFrom(src)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 0, src.numRecords())
		assert.Equal(t, 3, dest.numRecords()) // dest の 1 + src の 2
	})
}

func TestBranchNodeIsHalfFull(t *testing.T) {
	t.Run("空の場合は false を返す", func(t *testing.T) {
		// GIVEN
		bn := newUninitializedBranchNode()
		bn.body.initialize()

		// WHEN / THEN
		assert.False(t, bn.isHalfFull())
	})

	t.Run("半分以上埋まっている場合は true を返す", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode()
		for i := range 100 {
			key := make([]byte, 20)
			key[0] = byte(i/256 + 0x11)
			key[1] = byte(i % 256)
			bn.insert(bn.numRecords(), newBranchRecord(key, page.NewId(0, page.PageNumber(i+10))))
		}

		// WHEN / THEN
		assert.True(t, bn.isHalfFull())
	})
}

// newUninitializedBranchNode は未初期化の BranchNode を作成する
func newUninitializedBranchNode() *branchNode {
	pg, err := page.NewPage(make([]byte, page.Size))
	if err != nil {
		panic(err)
	}
	return newBranchNode(pg)
}

// newTestBranchNode は初期化済みの BranchNode を作成する (レコード 1 つ、key=0x10)
func newTestBranchNode() *branchNode {
	bn := newUninitializedBranchNode()
	_ = bn.initialize([]byte{0x10}, page.NewId(0, 1), page.NewId(0, 2))
	return bn
}

// newBranchRecord はブランチノード用のレコードを作成する
func newBranchRecord(key []byte, childPageId page.Id) Record {
	return NewRecord([]byte{}, key, childPageId.ToBytes())
}
