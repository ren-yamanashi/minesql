package node

import (
	"bytes"
	"fmt"
	"minesql/internal/storage/page"
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/assert"
)

func TestNewBranchNode(t *testing.T) {
	t.Run("ノードタイプが BRANCH に設定される", func(t *testing.T) {
		// GIVEN
		data := directio.AlignedBlock(directio.BlockSize)

		// WHEN
		bn := NewBranchNode(data)

		// THEN
		assert.NotNil(t, bn)
		assert.True(t, bytes.Equal(data[0:8], NODE_TYPE_BRANCH))
	})
}

func TestBranchNodeBody(t *testing.T) {
	t.Run("ノードタイプヘッダーを除いたボディ部分が取得できる", func(t *testing.T) {
		// GIVEN
		data := directio.AlignedBlock(directio.BlockSize)
		bn := NewBranchNode(data)

		// WHEN
		body := bn.Body()

		// THEN
		assert.Equal(t, len(data)-nodeHeaderSize, len(body))
	})
}

func TestBranchNodeNumPairs(t *testing.T) {
	t.Run("Initialize 直後はペア数が 1", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{NewPair([]byte("key1"), pageIdBytes(10))},
			page.NewPageId(0, 20),
		)

		// WHEN
		numPairs := bn.NumPairs()

		// THEN
		assert.Equal(t, 1, numPairs)
	})

	t.Run("挿入後のペア数が正しく取得できる", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("key1"), pageIdBytes(10)),
				NewPair([]byte("key2"), pageIdBytes(20)),
				NewPair([]byte("key3"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN
		numPairs := bn.NumPairs()

		// THEN
		assert.Equal(t, 3, numPairs)
	})
}

func TestBranchNodePairAt(t *testing.T) {
	t.Run("指定したスロット番号のペアが取得できる", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("key1"), pageIdBytes(10)),
				NewPair([]byte("key2"), pageIdBytes(20)),
			},
			page.NewPageId(0, 30),
		)

		// WHEN
		pair := bn.PairAt(1)

		// THEN
		assert.Equal(t, []byte("key2"), pair.Key)
		assert.Equal(t, pageIdBytes(20), pair.Value)
	})
}

func TestBranchNodeSearchSlotNum(t *testing.T) {
	t.Run("存在するキーの場合、スロット番号と true を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("aaa"), pageIdBytes(10)),
				NewPair([]byte("bbb"), pageIdBytes(20)),
				NewPair([]byte("ccc"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN
		slotNum, found := bn.SearchSlotNum([]byte("bbb"))

		// THEN
		assert.True(t, found)
		assert.Equal(t, 1, slotNum)
	})

	t.Run("存在しないキーの場合、挿入位置と false を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("aaa"), pageIdBytes(10)),
				NewPair([]byte("ccc"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN
		slotNum, found := bn.SearchSlotNum([]byte("bbb"))

		// THEN
		assert.False(t, found)
		assert.Equal(t, 1, slotNum)
	})

	t.Run("先頭より小さいキーの場合、挿入位置 0 と false を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("bbb"), pageIdBytes(10)),
				NewPair([]byte("ccc"), pageIdBytes(20)),
			},
			page.NewPageId(0, 30),
		)

		// WHEN
		slotNum, found := bn.SearchSlotNum([]byte("aaa"))

		// THEN
		assert.False(t, found)
		assert.Equal(t, 0, slotNum)
	})

	t.Run("末尾より大きいキーの場合、末尾の挿入位置と false を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("aaa"), pageIdBytes(10)),
				NewPair([]byte("bbb"), pageIdBytes(20)),
			},
			page.NewPageId(0, 30),
		)

		// WHEN
		slotNum, found := bn.SearchSlotNum([]byte("zzz"))

		// THEN
		assert.False(t, found)
		assert.Equal(t, 2, slotNum)
	})
}

func TestBranchNodeInsert(t *testing.T) {
	t.Run("ペアが正しく挿入できる", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{NewPair([]byte("aaa"), pageIdBytes(10))},
			page.NewPageId(0, 20),
		)

		// WHEN
		ok := bn.Insert(1, NewPair([]byte("bbb"), pageIdBytes(30)))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 2, bn.NumPairs())
		assert.Equal(t, []byte("bbb"), bn.PairAt(1).Key)
	})

	t.Run("中間位置へのペア挿入でスロットが正しくシフトされる", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("aaa"), pageIdBytes(10)),
				NewPair([]byte("ccc"), pageIdBytes(30)),
				NewPair([]byte("ddd"), pageIdBytes(40)),
			},
			page.NewPageId(0, 50),
		)

		// WHEN
		ok := bn.Insert(1, NewPair([]byte("bbb"), pageIdBytes(20)))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 4, bn.NumPairs())
		assert.Equal(t, []byte("aaa"), bn.PairAt(0).Key)
		assert.Equal(t, []byte("bbb"), bn.PairAt(1).Key)
		assert.Equal(t, []byte("ccc"), bn.PairAt(2).Key)
		assert.Equal(t, []byte("ddd"), bn.PairAt(3).Key)
	})

	t.Run("最大ペアサイズを超える場合、挿入に失敗する", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{NewPair([]byte("aaa"), pageIdBytes(10))},
			page.NewPageId(0, 20),
		)
		hugeKey := make([]byte, 4000)

		// WHEN
		ok := bn.Insert(1, NewPair(hugeKey, pageIdBytes(30)))

		// THEN
		assert.False(t, ok)
		assert.Equal(t, 1, bn.NumPairs())
	})

	t.Run("ページが満杯の場合、挿入に失敗し既存データが壊れない", func(t *testing.T) {
		// GIVEN: ノードをほぼ満杯にする
		bn := createTestBranchNodeEmpty()
		bn.body.Initialize()
		value := pageIdBytes(1)
		inserted := 0
		for {
			key := fmt.Appendf(nil, "k%04d", inserted)
			if !bn.Insert(inserted, NewPair(key, value)) {
				break
			}
			inserted++
		}
		numBefore := bn.NumPairs()
		firstKeyBefore := make([]byte, len(bn.PairAt(0).Key))
		copy(firstKeyBefore, bn.PairAt(0).Key)

		// WHEN: もう 1 つ挿入を試みる (満杯で入らなかったキーと同サイズ)
		overflowKey := fmt.Appendf(nil, "k%04d", inserted)
		ok := bn.Insert(0, NewPair(overflowKey, value))

		// THEN
		assert.False(t, ok)
		assert.Equal(t, numBefore, bn.NumPairs())
		assert.Equal(t, firstKeyBefore, bn.PairAt(0).Key)
	})
}

func TestBranchNodeInitialize(t *testing.T) {
	t.Run("初期化後にペア数が 1 で正しいキーと子ページ ID が設定される", func(t *testing.T) {
		// GIVEN
		data := directio.AlignedBlock(directio.BlockSize)
		bn := NewBranchNode(data)
		leftChild := page.NewPageId(0, 10)
		rightChild := page.NewPageId(0, 20)

		// WHEN
		err := bn.Initialize([]byte("key1"), leftChild, rightChild)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, bn.NumPairs())
		assert.Equal(t, []byte("key1"), bn.PairAt(0).Key)
		assert.Equal(t, leftChild, page.RestorePageIdFromBytes(bn.PairAt(0).Value))
		assert.Equal(t, rightChild, bn.RightChildPageId())
	})
}

func TestBranchNodeSearchChildSlotNum(t *testing.T) {
	t.Run("キーが見つかった場合、slotNum + 1 を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("aaa"), pageIdBytes(10)),
				NewPair([]byte("bbb"), pageIdBytes(20)),
				NewPair([]byte("ccc"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN
		childSlotNum := bn.SearchChildSlotNum([]byte("bbb"))

		// THEN
		assert.Equal(t, 2, childSlotNum)
	})

	t.Run("キーが見つからない場合、挿入位置をそのまま返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("aaa"), pageIdBytes(10)),
				NewPair([]byte("ccc"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN
		childSlotNum := bn.SearchChildSlotNum([]byte("bbb"))

		// THEN
		assert.Equal(t, 1, childSlotNum)
	})

	t.Run("すべてのキーより小さい場合、0 を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("bbb"), pageIdBytes(10)),
				NewPair([]byte("ccc"), pageIdBytes(20)),
			},
			page.NewPageId(0, 30),
		)

		// WHEN
		childSlotNum := bn.SearchChildSlotNum([]byte("aaa"))

		// THEN
		assert.Equal(t, 0, childSlotNum)
	})

	t.Run("すべてのキーより大きい場合、NumPairs を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("aaa"), pageIdBytes(10)),
				NewPair([]byte("bbb"), pageIdBytes(20)),
			},
			page.NewPageId(0, 30),
		)

		// WHEN
		childSlotNum := bn.SearchChildSlotNum([]byte("zzz"))

		// THEN
		assert.Equal(t, 2, childSlotNum)
	})
}

func TestBranchNodeChildPageIdAt(t *testing.T) {
	t.Run("通常のスロット番号の場合、ペアの value からページ ID を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("key1"), pageIdBytes(10)),
				NewPair([]byte("key2"), pageIdBytes(20)),
			},
			page.NewPageId(0, 30),
		)

		// WHEN
		childPageId := bn.ChildPageIdAt(0)

		// THEN
		assert.Equal(t, page.NewPageId(0, 10), childPageId)
	})

	t.Run("スロット番号が NumPairs と等しい場合、右端の子ページ ID を返す", func(t *testing.T) {
		// GIVEN
		rightChild := page.NewPageId(0, 99)
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("key1"), pageIdBytes(10)),
				NewPair([]byte("key2"), pageIdBytes(20)),
			},
			rightChild,
		)

		// WHEN
		childPageId := bn.ChildPageIdAt(2)

		// THEN
		assert.Equal(t, rightChild, childPageId)
	})
}

func TestBranchNodeSplitInsert(t *testing.T) {
	t.Run("昇順挿入で分割され、キー順序と minKey が正しい", func(t *testing.T) {
		// GIVEN: ブランチノードを昇順キーで満杯にする
		bn := createTestBranchNodeEmpty()
		bn.body.Initialize()
		value := pageIdBytes(1)
		numInserted := 0
		for {
			key := fmt.Appendf(nil, "k%04d", numInserted)
			if !bn.Insert(numInserted, NewPair(key, value)) {
				break
			}
			numInserted++
		}
		overflowKey := fmt.Appendf(nil, "k%04d", numInserted)
		newBn := createTestBranchNodeEmpty()

		// WHEN
		minKey, err := bn.SplitInsert(newBn, NewPair(overflowKey, value))

		// THEN
		assert.NoError(t, err)
		assert.Greater(t, bn.NumPairs(), 0)
		assert.Greater(t, newBn.NumPairs(), 0)
		assert.NotNil(t, minKey)

		// 新ノードの全キー < 旧ノードの全キー
		newLastKey := newBn.PairAt(newBn.NumPairs() - 1).Key
		oldFirstKey := bn.PairAt(0).Key
		assert.True(t, bytes.Compare(newLastKey, oldFirstKey) < 0)

		// 各ノード内のキーが昇順
		assertBranchKeysSorted(t, bn)
		assertBranchKeysSorted(t, newBn)

		// 新ノードの RightChildPageId が有効なページ ID である
		newRightChild := newBn.RightChildPageId()
		assert.NotEqual(t, page.PageId{}, newRightChild)
	})

	t.Run("既存の最小キーより小さいキーで分割できる", func(t *testing.T) {
		// GIVEN: ブランチノードを "b" 始まりのキーで満杯にする
		bn := createTestBranchNodeEmpty()
		bn.body.Initialize()
		value := pageIdBytes(1)
		numInserted := 0
		for {
			key := fmt.Appendf(nil, "b%04d", numInserted)
			if !bn.Insert(numInserted, NewPair(key, value)) {
				break
			}
			numInserted++
		}
		smallKey := []byte("a0000")
		newBn := createTestBranchNodeEmpty()

		// WHEN
		minKey, err := bn.SplitInsert(newBn, NewPair(smallKey, value))

		// THEN
		assert.NoError(t, err)
		assert.Greater(t, bn.NumPairs(), 0)
		assert.Greater(t, newBn.NumPairs(), 0)
		assert.NotNil(t, minKey)

		// 各ノード内のキーが昇順
		assertBranchKeysSorted(t, bn)
		assertBranchKeysSorted(t, newBn)

		// 新ノードの全キー < 旧ノードの全キー
		newLastKey := newBn.PairAt(newBn.NumPairs() - 1).Key
		oldFirstKey := bn.PairAt(0).Key
		assert.True(t, bytes.Compare(newLastKey, oldFirstKey) < 0)

		// 新ノードの RightChildPageId が有効なページ ID である
		newRightChild := newBn.RightChildPageId()
		assert.NotEqual(t, page.PageId{}, newRightChild)
	})
}

func TestBranchNodeDelete(t *testing.T) {
	t.Run("中間ペアの削除が正しく動作する", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("key1"), pageIdBytes(10)),
				NewPair([]byte("key2"), pageIdBytes(20)),
				NewPair([]byte("key3"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN
		bn.Delete(1)

		// THEN
		assert.Equal(t, 2, bn.NumPairs())
		assert.Equal(t, []byte("key1"), bn.PairAt(0).Key)
		assert.Equal(t, []byte("key3"), bn.PairAt(1).Key)
	})

	t.Run("先頭ペアの削除が正しく動作する", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("key1"), pageIdBytes(10)),
				NewPair([]byte("key2"), pageIdBytes(20)),
				NewPair([]byte("key3"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN
		bn.Delete(0)

		// THEN
		assert.Equal(t, 2, bn.NumPairs())
		assert.Equal(t, []byte("key2"), bn.PairAt(0).Key)
		assert.Equal(t, []byte("key3"), bn.PairAt(1).Key)
	})

	t.Run("末尾ペアの削除が正しく動作する", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("key1"), pageIdBytes(10)),
				NewPair([]byte("key2"), pageIdBytes(20)),
				NewPair([]byte("key3"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN
		bn.Delete(2)

		// THEN
		assert.Equal(t, 2, bn.NumPairs())
		assert.Equal(t, []byte("key1"), bn.PairAt(0).Key)
		assert.Equal(t, []byte("key2"), bn.PairAt(1).Key)
	})
}

func TestBranchNodeIsHalfFull(t *testing.T) {
	t.Run("ペアが十分にある場合、true を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNodeEmpty()
		bigKey := make([]byte, 1500)
		bn.body.Initialize()
		bn.Insert(0, NewPair(bigKey, pageIdBytes(10)))
		copy(bigKey, []byte("key2"))
		bn.Insert(1, NewPair(bigKey, pageIdBytes(20)))

		// WHEN
		result := bn.IsHalfFull()

		// THEN
		assert.True(t, result)
	})

	t.Run("ペアが少ない場合、false を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNodeEmpty()
		bn.body.Initialize()
		bn.Insert(0, NewPair([]byte("k"), pageIdBytes(10)))

		// WHEN
		result := bn.IsHalfFull()

		// THEN
		assert.False(t, result)
	})
}

func TestBranchNodeCanTransferPair(t *testing.T) {
	t.Run("ペアが 0 の場合、false を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNodeEmpty()
		bn.body.Initialize()

		// WHEN
		result := bn.CanTransferPair(false)

		// THEN
		assert.False(t, result)
	})

	t.Run("ペアが 1 つしかない場合、false を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{NewPair([]byte("key1"), pageIdBytes(10))},
			page.NewPageId(0, 20),
		)

		// WHEN
		result := bn.CanTransferPair(false)

		// THEN
		assert.False(t, result)
	})

	t.Run("先頭ペアを転送後も半分以上埋まっている場合、true を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNodeEmpty()
		bn.body.Initialize()
		bigKey := make([]byte, 500)
		for i := range 6 {
			copy(bigKey, fmt.Appendf(nil, "key%d", i))
			bn.Insert(i, NewPair(bigKey, pageIdBytes(page.PageNumber(i*10))))
		}

		// WHEN
		result := bn.CanTransferPair(false)

		// THEN
		assert.True(t, result)
	})

	t.Run("末尾ペアを転送後も半分以上埋まっている場合、true を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNodeEmpty()
		bn.body.Initialize()
		bigKey := make([]byte, 500)
		for i := range 6 {
			copy(bigKey, fmt.Appendf(nil, "key%d", i))
			bn.Insert(i, NewPair(bigKey, pageIdBytes(page.PageNumber(i*10))))
		}

		// WHEN
		result := bn.CanTransferPair(true)

		// THEN
		assert.True(t, result)
	})

	t.Run("転送後に半分を下回る場合、false を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNodeEmpty()
		bn.body.Initialize()
		bigKey := make([]byte, 500)
		copy(bigKey, []byte("key1"))
		bn.Insert(0, NewPair(bigKey, pageIdBytes(10)))
		copy(bigKey, []byte("key2"))
		bn.Insert(1, NewPair(bigKey, pageIdBytes(20)))

		// WHEN
		result := bn.CanTransferPair(false)

		// THEN
		assert.False(t, result)
	})
}

func TestBranchNodeUpdateKeyAt(t *testing.T) {
	t.Run("指定したスロットのキーが更新される", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("aaa"), pageIdBytes(10)),
				NewPair([]byte("bbb"), pageIdBytes(20)),
				NewPair([]byte("ddd"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN: key "bbb" を同じ長さの "ccc" に更新
		bn.UpdateKeyAt(1, []byte("ccc"))

		// THEN
		assert.Equal(t, 3, bn.NumPairs())
		assert.Equal(t, []byte("ccc"), bn.PairAt(1).Key)
	})

	t.Run("更新後もペア数や他のキーに影響がない", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("aaa"), pageIdBytes(10)),
				NewPair([]byte("bbb"), pageIdBytes(20)),
			},
			page.NewPageId(0, 30),
		)

		// WHEN
		bn.UpdateKeyAt(0, []byte("aab"))

		// THEN
		assert.Equal(t, 2, bn.NumPairs())
		assert.Equal(t, []byte("aab"), bn.PairAt(0).Key)
		assert.Equal(t, []byte("bbb"), bn.PairAt(1).Key)
	})

	t.Run("異なる長さのキーで更新しても正しく動作する", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{
				NewPair([]byte("aaa"), pageIdBytes(10)),
				NewPair([]byte("bbb"), pageIdBytes(20)),
				NewPair([]byte("ccc"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN: 3 バイトのキーを 6 バイトのキーに更新
		bn.UpdateKeyAt(1, []byte("bbbbbb"))

		// THEN
		assert.Equal(t, 3, bn.NumPairs())
		assert.Equal(t, []byte("aaa"), bn.PairAt(0).Key)
		assert.Equal(t, []byte("bbbbbb"), bn.PairAt(1).Key)
		assert.Equal(t, []byte("ccc"), bn.PairAt(2).Key)
		// value も壊れていないことを確認
		assert.Equal(t, pageIdBytes(10), bn.PairAt(0).Value)
		assert.Equal(t, pageIdBytes(30), bn.PairAt(2).Value)
	})
}

func TestBranchNodeRightChildPageId(t *testing.T) {
	t.Run("右端の子ページ ID が正しく取得できる", func(t *testing.T) {
		// GIVEN
		rightChild := page.NewPageId(0, 99)
		bn := createTestBranchNode(
			[]Pair{NewPair([]byte("key1"), pageIdBytes(10))},
			rightChild,
		)

		// WHEN
		result := bn.RightChildPageId()

		// THEN
		assert.Equal(t, rightChild, result)
	})
}

func TestBranchNodeSetRightChildPageId(t *testing.T) {
	t.Run("右端の子ページ ID が正しく設定できる", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchNode(
			[]Pair{NewPair([]byte("key1"), pageIdBytes(10))},
			page.NewPageId(0, 40),
		)
		newRightChild := page.NewPageId(0, 200)

		// WHEN
		bn.SetRightChildPageId(newRightChild)

		// THEN
		assert.Equal(t, newRightChild, bn.RightChildPageId())
	})
}

func TestBranchNodeTransferAllFrom(t *testing.T) {
	t.Run("すべてのペアが転送元から自分に移動する", func(t *testing.T) {
		// GIVEN
		src := createTestBranchNode(
			[]Pair{
				NewPair([]byte("key3"), pageIdBytes(30)),
				NewPair([]byte("key4"), pageIdBytes(40)),
			},
			page.NewPageId(0, 50),
		)
		dest := createTestBranchNode(
			[]Pair{
				NewPair([]byte("key1"), pageIdBytes(10)),
			},
			page.NewPageId(0, 20),
		)

		// WHEN
		dest.TransferAllFrom(src)

		// THEN
		assert.Equal(t, 0, src.NumPairs())
		assert.Equal(t, 3, dest.NumPairs())
		assert.Equal(t, []byte("key1"), dest.PairAt(0).Key)
		assert.Equal(t, []byte("key3"), dest.PairAt(1).Key)
		assert.Equal(t, []byte("key4"), dest.PairAt(2).Key)
	})

	t.Run("転送元が空の場合、転送先のデータが変わらない", func(t *testing.T) {
		// GIVEN
		src := createTestBranchNodeEmpty()
		src.body.Initialize()
		dest := createTestBranchNode(
			[]Pair{
				NewPair([]byte("key1"), pageIdBytes(10)),
			},
			page.NewPageId(0, 20),
		)

		// WHEN
		dest.TransferAllFrom(src)

		// THEN
		assert.Equal(t, 0, src.NumPairs())
		assert.Equal(t, 1, dest.NumPairs())
		assert.Equal(t, []byte("key1"), dest.PairAt(0).Key)
	})
}

// テスト用のブランチノードを作成する (ペアあり)
func createTestBranchNode(pairs []Pair, rightChildPageId page.PageId) *BranchNode {
	data := directio.AlignedBlock(directio.BlockSize)
	bn := NewBranchNode(data)

	if len(pairs) == 0 {
		panic("pairs must not be empty")
	}

	err := bn.Initialize(pairs[0].Key, page.RestorePageIdFromBytes(pairs[0].Value), rightChildPageId)
	if err != nil {
		panic("failed to initialize branch node")
	}

	for i, pair := range pairs[1:] {
		if !bn.Insert(i+1, pair) {
			panic("failed to insert pair into branch node")
		}
	}

	return bn
}

// テスト用の空のブランチノードを作成する
func createTestBranchNodeEmpty() *BranchNode {
	data := directio.AlignedBlock(directio.BlockSize)
	return NewBranchNode(data)
}

// ブランチノード内のキーが昇順であることを検証するヘルパー
func assertBranchKeysSorted(t *testing.T, bn *BranchNode) {
	t.Helper()
	for i := 1; i < bn.NumPairs(); i++ {
		prev := bn.PairAt(i - 1).Key
		curr := bn.PairAt(i).Key
		assert.True(t, bytes.Compare(prev, curr) < 0,
			"キーが昇順でない: index %d (%s) >= index %d (%s)", i-1, prev, i, curr)
	}
}

// page.PageId を ToBytes() するヘルパー
func pageIdBytes(pageNum page.PageNumber) []byte {
	pid := page.NewPageId(0, pageNum)
	return pid.ToBytes()
}
