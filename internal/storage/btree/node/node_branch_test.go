package node

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestBranchNodeInitialize(t *testing.T) {
	t.Run("初期化後にレコードが 1 つ挿入され右の子が設定される", func(t *testing.T) {
		// GIVEN
		bn := newUninitializedBranchNode(256)
		leftChild := page.NewPageId(0, 1)
		rightChild := page.NewPageId(0, 2)

		// WHEN
		err := bn.Initialize([]byte{0x10}, leftChild, rightChild)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, bn.NumRecords())
		assert.Equal(t, []byte{0x10}, bn.Record(0).Key())
		assert.Equal(t, rightChild, bn.RightChildPageId())
	})
}

func TestBranchNodeInsert(t *testing.T) {
	t.Run("レコードを挿入できる", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode(256)
		record := newBranchRecord([]byte{0x20}, page.NewPageId(0, 10))

		// WHEN
		ok := bn.Insert(1, record)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 2, bn.NumRecords())
	})

	t.Run("maxRecordSize を超えるレコードは挿入できない", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode(64)
		maxSize := bn.maxRecordSize()
		largeKey := make([]byte, maxSize)

		// WHEN
		ok := bn.Insert(1, NewRecord([]byte{}, largeKey, page.NewPageId(0, 1).ToBytes()))

		// THEN
		assert.False(t, ok)
	})
}

func TestBranchNodeSplitInsert(t *testing.T) {
	t.Run("挿入キーが先頭キーより大きい場合に分割できる", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode(128)
		for i := range 6 {
			bn.Insert(bn.NumRecords(), newBranchRecord([]byte{byte(i + 0x11)}, page.NewPageId(0, page.PageNumber(i+10))))
		}
		newBranch := newUninitializedBranchNode(128)
		newRecord := newBranchRecord([]byte{0xFF}, page.NewPageId(0, 99))

		// WHEN
		key, err := bn.SplitInsert(newBranch, newRecord)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, key)
		assert.True(t, bn.NumRecords() > 0)
		assert.True(t, newBranch.NumRecords() > 0)
	})

	t.Run("挿入キーが先頭キー以下の場合に分割できる", func(t *testing.T) {
		// GIVEN
		bn := newUninitializedBranchNode(256)
		bn.Initialize([]byte{0x10}, page.NewPageId(0, 1), page.NewPageId(0, 2))
		for i := range 9 {
			bn.Insert(bn.NumRecords(), newBranchRecord([]byte{byte(i + 0x11)}, page.NewPageId(0, page.PageNumber(i+10))))
		}
		newBranch := newUninitializedBranchNode(256)
		newRecord := newBranchRecord([]byte{0x01}, page.NewPageId(0, 99))

		// WHEN
		key, err := bn.SplitInsert(newBranch, newRecord)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, key)
		assert.True(t, bn.NumRecords() > 0)
		assert.True(t, newBranch.NumRecords() > 0)
	})

	t.Run("分割後に古いノードの容量が不足するとエラーを返す", func(t *testing.T) {
		// GIVEN
		bn := newUninitializedBranchNode(128)
		maxSize := bn.maxRecordSize()
		bigKey := make([]byte, maxSize-12)
		bigKey[0] = 0x01
		bn.Initialize(bigKey, page.NewPageId(0, 1), page.NewPageId(0, 2))
		bigKey2 := make([]byte, maxSize-12)
		bigKey2[0] = 0x02
		bn.Insert(bn.NumRecords(), newBranchRecord(bigKey2, page.NewPageId(0, 10)))
		newBranch := newUninitializedBranchNode(128)
		bigKey3 := make([]byte, maxSize-12)
		bigKey3[0] = 0x01
		bigKey3[1] = 0x01
		newRecord := newBranchRecord(bigKey3, page.NewPageId(0, 99))

		// WHEN
		key, err := bn.SplitInsert(newBranch, newRecord)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, key)
	})

	t.Run("挿入キーが大きい場合に転送先の容量が不足するとエラーを返す", func(t *testing.T) {
		// GIVEN
		bn := newUninitializedBranchNode(256)
		bn.Initialize([]byte{0x10}, page.NewPageId(0, 1), page.NewPageId(0, 2))
		for i := range 9 {
			bn.Insert(bn.NumRecords(), newBranchRecord([]byte{byte(i + 0x11)}, page.NewPageId(0, page.PageNumber(i+10))))
		}
		newBranch := newUninitializedBranchNode(48)
		newRecord := newBranchRecord([]byte{0x04, 0x01}, page.NewPageId(0, 99))

		// WHEN
		key, err := bn.SplitInsert(newBranch, newRecord)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, key)
	})

	t.Run("挿入キーが小さく転送先に Insert できない場合はエラーを返す", func(t *testing.T) {
		// GIVEN
		bn := newUninitializedBranchNode(256)
		bn.Initialize([]byte{0x10}, page.NewPageId(0, 1), page.NewPageId(0, 2))
		for i := range 9 {
			bn.Insert(bn.NumRecords(), newBranchRecord([]byte{byte(i + 0x11)}, page.NewPageId(0, page.PageNumber(i+10))))
		}
		newBranch := newUninitializedBranchNode(32)
		newRecord := newBranchRecord([]byte{0x01}, page.NewPageId(0, 99))

		// WHEN
		key, err := bn.SplitInsert(newBranch, newRecord)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, key)
	})

	t.Run("挿入キーが小さい場合に転送先の容量が不足するとエラーを返す", func(t *testing.T) {
		// GIVEN
		bn := newUninitializedBranchNode(256)
		bn.Initialize([]byte{0x10}, page.NewPageId(0, 1), page.NewPageId(0, 2))
		for i := range 9 {
			bn.Insert(bn.NumRecords(), newBranchRecord([]byte{byte(i + 0x11)}, page.NewPageId(0, page.PageNumber(i+10))))
		}
		// newBranch は Insert 1 回分は入るが、transfer で溢れるサイズ
		newBranch := newUninitializedBranchNode(64)
		newRecord := newBranchRecord([]byte{0x01}, page.NewPageId(0, 99))

		// WHEN
		key, err := bn.SplitInsert(newBranch, newRecord)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, key)
	})
}

func TestBranchNodeRemove(t *testing.T) {
	t.Run("レコードを削除できる", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode(256)
		bn.Insert(1, newBranchRecord([]byte{0x20}, page.NewPageId(0, 10)))

		// WHEN
		bn.Remove(1)

		// THEN
		assert.Equal(t, 1, bn.NumRecords())
	})
}

func TestBranchNodeUpdate(t *testing.T) {
	t.Run("レコードを更新できる", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode(256)
		newRecord := newBranchRecord([]byte{0xFF}, page.NewPageId(0, 99))

		// WHEN
		ok := bn.Update(0, newRecord)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte{0xFF}, bn.Record(0).Key())
	})
}

func TestBranchNodeNumRecords(t *testing.T) {
	t.Run("レコード数を返す", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode(256)
		bn.Insert(1, newBranchRecord([]byte{0x20}, page.NewPageId(0, 10)))

		// WHEN / THEN
		assert.Equal(t, 2, bn.NumRecords())
	})
}

func TestBranchNodeCanTransferRecord(t *testing.T) {
	t.Run("レコードが 1 つ以下の場合は false を返す", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode(256)

		// WHEN / THEN
		assert.False(t, bn.CanTransferRecord(true))
		assert.False(t, bn.CanTransferRecord(false))
	})

	t.Run("転送後も半分以上埋まっている場合は true を返す", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode(128)
		for i := range 8 {
			bn.Insert(bn.NumRecords(), newBranchRecord([]byte{byte(i + 2)}, page.NewPageId(0, page.PageNumber(i+10))))
		}

		// WHEN / THEN
		assert.True(t, bn.CanTransferRecord(true))
		assert.True(t, bn.CanTransferRecord(false))
	})
}

func TestBranchNodeRecordAt(t *testing.T) {
	t.Run("指定したスロット番号のレコードを取得できる", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode(256)
		bn.Insert(1, newBranchRecord([]byte{0x20}, page.NewPageId(0, 10)))

		// WHEN
		r := bn.Record(1)

		// THEN
		assert.Equal(t, []byte{0x20}, r.Key())
	})
}

func TestBranchNodeSearchSlotNum(t *testing.T) {
	t.Run("キーが見つかった場合はスロット番号と true を返す", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode(256)
		bn.Insert(1, newBranchRecord([]byte{0x20}, page.NewPageId(0, 10)))

		// WHEN
		slotNum, found := bn.SearchSlotNum([]byte{0x20})

		// THEN
		assert.Equal(t, 1, slotNum)
		assert.True(t, found)
	})

	t.Run("キーが見つからない場合は挿入位置と false を返す", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode(256)
		bn.Insert(1, newBranchRecord([]byte{0x30}, page.NewPageId(0, 10)))

		// WHEN
		slotNum, found := bn.SearchSlotNum([]byte{0x20})

		// THEN
		assert.Equal(t, 1, slotNum)
		assert.False(t, found)
	})
}

func TestBranchNodeChildPageId(t *testing.T) {
	t.Run("通常のスロット番号の場合はレコードの NonKey から PageId を返す", func(t *testing.T) {
		// GIVEN
		childId := page.NewPageId(0, 10)
		bn := newTestBranchNode(256)
		bn.Insert(1, newBranchRecord([]byte{0x20}, childId))

		// WHEN
		id, err := bn.ChildPageId(1)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, childId, id)
	})

	t.Run("スロット番号がレコード数と同じ場合は右の子の PageId を返す", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode(256)

		// WHEN
		id, err := bn.ChildPageId(bn.NumRecords())

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, bn.RightChildPageId(), id)
	})
}

func TestBranchNodeRightChildPageId(t *testing.T) {
	t.Run("Initialize で設定した右の子の PageId を返す", func(t *testing.T) {
		// GIVEN
		rightChild := page.NewPageId(0, 2)
		bn := newUninitializedBranchNode(256)
		bn.Initialize([]byte{0x10}, page.NewPageId(0, 1), rightChild)

		// WHEN
		id := bn.RightChildPageId()

		// THEN
		assert.Equal(t, rightChild, id)
	})
}

func TestBranchNodeSetRightChildPageId(t *testing.T) {
	t.Run("右の子の PageId を更新できる", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode(256)
		newId := page.NewPageId(1, 99)

		// WHEN
		bn.SetRightChildPageId(newId)

		// THEN
		assert.Equal(t, newId, bn.RightChildPageId())
	})
}

func TestBranchNodeTransferAllFrom(t *testing.T) {
	t.Run("全レコードを転送できる", func(t *testing.T) {
		// GIVEN
		src := newTestBranchNode(256)
		src.Insert(1, newBranchRecord([]byte{0x20}, page.NewPageId(0, 10)))
		dest := newTestBranchNode(256)

		// WHEN
		ok := dest.TransferAllFrom(src)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 0, src.NumRecords())
		assert.Equal(t, 3, dest.NumRecords()) // dest の 1 + src の 2
	})
}

func TestBranchNodeIsHalfFull(t *testing.T) {
	t.Run("空の場合は false を返す", func(t *testing.T) {
		// GIVEN
		bn := newUninitializedBranchNode(256)
		bn.body.Initialize()

		// WHEN / THEN
		assert.False(t, bn.IsHalfFull())
	})

	t.Run("半分以上埋まっている場合は true を返す", func(t *testing.T) {
		// GIVEN
		bn := newTestBranchNode(128)
		for i := range 8 {
			bn.Insert(bn.NumRecords(), newBranchRecord([]byte{byte(i + 2)}, page.NewPageId(0, page.PageNumber(i+10))))
		}

		// WHEN / THEN
		assert.True(t, bn.IsHalfFull())
	})
}

// newUninitializedBranchNode は未初期化の BranchNode を作成する
func newUninitializedBranchNode(size int) *BranchNode {
	data := make([]byte, size)
	pg, err := page.NewPage(data)
	if err != nil {
		panic(err)
	}
	return NewBranchNode(pg)
}

// newTestBranchNode は初期化済みの BranchNode を作成する (レコード 1 つ、key=0x10)
func newTestBranchNode(size int) *BranchNode {
	bn := newUninitializedBranchNode(size)
	bn.Initialize([]byte{0x10}, page.NewPageId(0, 1), page.NewPageId(0, 2))
	return bn
}

// newBranchRecord はブランチノード用のレコードを作成する
func newBranchRecord(key []byte, childPageId page.PageId) Record {
	return NewRecord([]byte{}, key, childPageId.ToBytes())
}
