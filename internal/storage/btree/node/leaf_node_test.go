package node

import (
	"bytes"
	"fmt"
	"minesql/internal/storage/page"
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/assert"
)

func TestNewLeafNode(t *testing.T) {
	t.Run("ノードタイプが LEAF に設定される", func(t *testing.T) {
		// GIVEN
		data := directio.AlignedBlock(directio.BlockSize)

		// WHEN
		ln := NewLeafNode(data)

		// THEN
		assert.NotNil(t, ln)
		assert.True(t, bytes.Equal(data[0:8], NODE_TYPE_LEAF))
	})
}

func TestLeafNodeInitialize(t *testing.T) {
	t.Run("初期化後にペア数が 0、前後ページ ID が nil になる", func(t *testing.T) {
		// GIVEN
		data := directio.AlignedBlock(directio.BlockSize)
		ln := NewLeafNode(data)

		// WHEN
		ln.Initialize()

		// THEN
		assert.Equal(t, 0, ln.NumPairs())
		assert.Nil(t, ln.PrevPageId())
		assert.Nil(t, ln.NextPageId())
	})
}

func TestLeafNodeInsert(t *testing.T) {
	t.Run("ペアが正しく挿入できる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)

		// WHEN
		ok := ln.Insert(0, NewPair([]byte("key1"), []byte("val1")))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 1, ln.NumPairs())
		assert.Equal(t, []byte("key1"), ln.PairAt(0).Key)
		assert.Equal(t, []byte("val1"), ln.PairAt(0).Value)
	})

	t.Run("中間位置へのペア挿入でスロットが正しくシフトされる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Pair{
			NewPair([]byte("aaa"), []byte("v1")),
			NewPair([]byte("ccc"), []byte("v3")),
			NewPair([]byte("ddd"), []byte("v4")),
		})

		// WHEN
		ok := ln.Insert(1, NewPair([]byte("bbb"), []byte("v2")))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 4, ln.NumPairs())
		assert.Equal(t, []byte("aaa"), ln.PairAt(0).Key)
		assert.Equal(t, []byte("bbb"), ln.PairAt(1).Key)
		assert.Equal(t, []byte("ccc"), ln.PairAt(2).Key)
		assert.Equal(t, []byte("ddd"), ln.PairAt(3).Key)
	})

	t.Run("最大ペアサイズを超える場合、挿入に失敗する", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		hugeValue := make([]byte, 4000)

		// WHEN
		ok := ln.Insert(0, NewPair([]byte("key"), hugeValue))

		// THEN
		assert.False(t, ok)
		assert.Equal(t, 0, ln.NumPairs())
	})

	t.Run("ページが満杯の場合、挿入に失敗し既存データが壊れない", func(t *testing.T) {
		// GIVEN: ノードをほぼ満杯にする
		ln := createTestLeafNode(nil)
		value := make([]byte, 200)
		inserted := 0
		for {
			key := fmt.Appendf(nil, "k%02d", inserted)
			if !ln.Insert(inserted, NewPair(key, value)) {
				break
			}
			inserted++
		}
		numBefore := ln.NumPairs()
		firstKeyBefore := make([]byte, len(ln.PairAt(0).Key))
		copy(firstKeyBefore, ln.PairAt(0).Key)

		// WHEN: もう 1 つ挿入を試みる (小さいペアだが容量不足)
		ok := ln.Insert(0, NewPair([]byte("new"), value))

		// THEN
		assert.False(t, ok)
		assert.Equal(t, numBefore, ln.NumPairs())
		assert.Equal(t, firstKeyBefore, ln.PairAt(0).Key)
	})
}

func TestLeafNodeSplitInsert(t *testing.T) {
	t.Run("昇順挿入で分割され、キー順序と minKey が正しい", func(t *testing.T) {
		// GIVEN: リーフノードを昇順キーで満杯にする
		ln := createTestLeafNode(nil)
		value := make([]byte, 200)
		numInserted := 0
		for {
			key := fmt.Appendf(nil, "k%02d", numInserted)
			if !ln.Insert(numInserted, NewPair(key, value)) {
				break
			}
			numInserted++
		}
		overflowKey := fmt.Appendf(nil, "k%02d", numInserted)
		newLn := createTestLeafNode(nil)

		// WHEN
		minKey, err := ln.SplitInsert(newLn, NewPair(overflowKey, value))

		// THEN
		assert.NoError(t, err)
		assert.Greater(t, ln.NumPairs(), 0)
		assert.Greater(t, newLn.NumPairs(), 0)
		assert.Equal(t, numInserted+1, ln.NumPairs()+newLn.NumPairs())

		// minKey が新ノードの先頭キーと一致する
		assert.Equal(t, newLn.PairAt(0).Key, minKey)

		// 新ノードの全キー < 旧ノードの全キー
		newLastKey := newLn.PairAt(newLn.NumPairs() - 1).Key
		oldFirstKey := ln.PairAt(0).Key
		assert.True(t, bytes.Compare(newLastKey, oldFirstKey) < 0)

		// 各ノード内のキーが昇順
		assertKeysSorted(t, ln)
		assertKeysSorted(t, newLn)
	})

	t.Run("既存の最小キーより小さいキーで分割できる", func(t *testing.T) {
		// GIVEN: リーフノードを "b" 始まりのキーで満杯にする
		ln := createTestLeafNode(nil)
		value := make([]byte, 200)
		numInserted := 0
		for {
			key := fmt.Appendf(nil, "b%02d", numInserted)
			if !ln.Insert(numInserted, NewPair(key, value)) {
				break
			}
			numInserted++
		}
		// 既存の最小キーより小さいキーで SplitInsert
		smallKey := []byte("a00")
		newLn := createTestLeafNode(nil)

		// WHEN
		minKey, err := ln.SplitInsert(newLn, NewPair(smallKey, value))

		// THEN
		assert.NoError(t, err)
		assert.Greater(t, ln.NumPairs(), 0)
		assert.Greater(t, newLn.NumPairs(), 0)
		assert.NotNil(t, minKey)
		assert.Equal(t, newLn.PairAt(0).Key, minKey)

		// 各ノード内のキーが昇順
		assertKeysSorted(t, ln)
		assertKeysSorted(t, newLn)

		// 新ノードの全キー < 旧ノードの全キー
		newLastKey := newLn.PairAt(newLn.NumPairs() - 1).Key
		oldFirstKey := ln.PairAt(0).Key
		assert.True(t, bytes.Compare(newLastKey, oldFirstKey) < 0)
	})
}

func TestLeafNodeDelete(t *testing.T) {
	t.Run("中間ペアの削除が正しく動作する", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Pair{
			NewPair([]byte("key1"), []byte("val1")),
			NewPair([]byte("key2"), []byte("val2")),
			NewPair([]byte("key3"), []byte("val3")),
		})

		// WHEN
		ln.Delete(1)

		// THEN
		assert.Equal(t, 2, ln.NumPairs())
		assert.Equal(t, []byte("key1"), ln.PairAt(0).Key)
		assert.Equal(t, []byte("key3"), ln.PairAt(1).Key)
	})

	t.Run("先頭ペアの削除が正しく動作する", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Pair{
			NewPair([]byte("key1"), []byte("val1")),
			NewPair([]byte("key2"), []byte("val2")),
			NewPair([]byte("key3"), []byte("val3")),
		})

		// WHEN
		ln.Delete(0)

		// THEN
		assert.Equal(t, 2, ln.NumPairs())
		assert.Equal(t, []byte("key2"), ln.PairAt(0).Key)
		assert.Equal(t, []byte("key3"), ln.PairAt(1).Key)
	})

	t.Run("末尾ペアの削除が正しく動作する", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Pair{
			NewPair([]byte("key1"), []byte("val1")),
			NewPair([]byte("key2"), []byte("val2")),
			NewPair([]byte("key3"), []byte("val3")),
		})

		// WHEN
		ln.Delete(2)

		// THEN
		assert.Equal(t, 2, ln.NumPairs())
		assert.Equal(t, []byte("key1"), ln.PairAt(0).Key)
		assert.Equal(t, []byte("key2"), ln.PairAt(1).Key)
	})

	t.Run("唯一のペアを削除するとペア数が 0 になる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Pair{
			NewPair([]byte("key1"), []byte("val1")),
		})

		// WHEN
		ln.Delete(0)

		// THEN
		assert.Equal(t, 0, ln.NumPairs())
	})
}

func TestLeafNodeUpdate(t *testing.T) {
	t.Run("value を更新できる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Pair{
			NewPair([]byte("key1"), []byte("val1")),
			NewPair([]byte("key2"), []byte("val2")),
			NewPair([]byte("key3"), []byte("val3")),
		})

		// WHEN
		ok := ln.Update(1, NewPair([]byte("key2"), []byte("updated")))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 3, ln.NumPairs())
		assert.Equal(t, []byte("key2"), ln.PairAt(1).Key)
		assert.Equal(t, []byte("updated"), ln.PairAt(1).Value)
	})

	t.Run("value を短い値に更新できる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Pair{
			NewPair([]byte("key1"), []byte("long_value_here")),
		})

		// WHEN
		ok := ln.Update(0, NewPair([]byte("key1"), []byte("short")))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte("key1"), ln.PairAt(0).Key)
		assert.Equal(t, []byte("short"), ln.PairAt(0).Value)
	})

	t.Run("value を長い値に更新できる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Pair{
			NewPair([]byte("key1"), []byte("short")),
		})

		// WHEN
		ok := ln.Update(0, NewPair([]byte("key1"), []byte("much_longer_value")))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte("key1"), ln.PairAt(0).Key)
		assert.Equal(t, []byte("much_longer_value"), ln.PairAt(0).Value)
	})

	t.Run("更新後も他のペアが壊れない", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Pair{
			NewPair([]byte("key1"), []byte("val1")),
			NewPair([]byte("key2"), []byte("val2")),
			NewPair([]byte("key3"), []byte("val3")),
		})

		// WHEN
		ok := ln.Update(1, NewPair([]byte("key2"), []byte("new_longer_value")))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte("key1"), ln.PairAt(0).Key)
		assert.Equal(t, []byte("val1"), ln.PairAt(0).Value)
		assert.Equal(t, []byte("key3"), ln.PairAt(2).Key)
		assert.Equal(t, []byte("val3"), ln.PairAt(2).Value)
	})

	t.Run("空き容量が不足している場合、false を返す", func(t *testing.T) {
		// GIVEN: ノードをほぼ満杯にする
		ln := createTestLeafNode(nil)
		value := make([]byte, 200)
		inserted := 0
		for {
			key := fmt.Appendf(nil, "k%02d", inserted)
			if !ln.Insert(inserted, NewPair(key, value)) {
				break
			}
			inserted++
		}
		originalValue := make([]byte, len(ln.PairAt(0).Value))
		copy(originalValue, ln.PairAt(0).Value)

		// WHEN: 非常に大きな値に更新を試みる
		hugeValue := make([]byte, 3000)
		ok := ln.Update(0, NewPair(ln.PairAt(0).Key, hugeValue))

		// THEN
		assert.False(t, ok)
	})
}

func TestLeafNodeCanTransferPair(t *testing.T) {
	t.Run("ペアが 0 の場合、false を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)

		// WHEN
		result := ln.CanTransferPair(false)

		// THEN
		assert.False(t, result)
	})

	t.Run("ペアが 1 つしかない場合、false を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		bigValue := make([]byte, 1000)
		ln.Insert(0, NewPair([]byte("key1"), bigValue))

		// WHEN
		result := ln.CanTransferPair(false)

		// THEN
		assert.False(t, result)
	})

	t.Run("左の兄弟に転送 (先頭ペアを転送) 後も半分以上埋まっている場合、true を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		bigValue := make([]byte, 500)
		ln.Insert(0, NewPair([]byte("key1"), bigValue))
		ln.Insert(1, NewPair([]byte("key2"), bigValue))
		ln.Insert(2, NewPair([]byte("key3"), bigValue))
		ln.Insert(3, NewPair([]byte("key4"), bigValue))
		ln.Insert(4, NewPair([]byte("key5"), bigValue))
		ln.Insert(5, NewPair([]byte("key6"), bigValue))

		// WHEN
		result := ln.CanTransferPair(false)

		// THEN
		assert.True(t, result)
	})

	t.Run("右の兄弟に転送 (末尾ペアを転送) 後も半分以上埋まっている場合、true を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		bigValue := make([]byte, 500)
		ln.Insert(0, NewPair([]byte("key1"), bigValue))
		ln.Insert(1, NewPair([]byte("key2"), bigValue))
		ln.Insert(2, NewPair([]byte("key3"), bigValue))
		ln.Insert(3, NewPair([]byte("key4"), bigValue))
		ln.Insert(4, NewPair([]byte("key5"), bigValue))
		ln.Insert(5, NewPair([]byte("key6"), bigValue))

		// WHEN
		result := ln.CanTransferPair(true)

		// THEN
		assert.True(t, result)
	})

	t.Run("転送後に半分を下回る場合、false を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		bigValue := make([]byte, 500)
		ln.Insert(0, NewPair([]byte("key1"), bigValue))
		ln.Insert(1, NewPair([]byte("key2"), bigValue))

		// WHEN
		result := ln.CanTransferPair(false)

		// THEN
		assert.False(t, result)
	})
}

func TestLeafNodeBody(t *testing.T) {
	t.Run("ノードタイプヘッダーを除いたボディ部分が取得できる", func(t *testing.T) {
		// GIVEN
		data := directio.AlignedBlock(directio.BlockSize)
		ln := NewLeafNode(data)

		// WHEN
		body := ln.Body()

		// THEN: data[8:] と同じスライスが返る
		assert.Equal(t, len(data)-nodeHeaderSize, len(body))
	})
}

func TestLeafNodeNumPairs(t *testing.T) {
	t.Run("初期化直後はペア数が 0", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)

		// WHEN
		numPairs := ln.NumPairs()

		// THEN
		assert.Equal(t, 0, numPairs)
	})

	t.Run("挿入後のペア数が正しく取得できる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Pair{
			NewPair([]byte("key1"), []byte("val1")),
			NewPair([]byte("key2"), []byte("val2")),
		})

		// WHEN
		numPairs := ln.NumPairs()

		// THEN
		assert.Equal(t, 2, numPairs)
	})
}

func TestLeafNodePairAt(t *testing.T) {
	t.Run("指定したスロット番号のペアが取得できる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Pair{
			NewPair([]byte("key1"), []byte("val1")),
			NewPair([]byte("key2"), []byte("val2")),
		})

		// WHEN
		pair := ln.PairAt(1)

		// THEN
		assert.Equal(t, []byte("key2"), pair.Key)
		assert.Equal(t, []byte("val2"), pair.Value)
	})
}

func TestLeafNodeSearchSlotNum(t *testing.T) {
	t.Run("存在するキーの場合、スロット番号と true を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Pair{
			NewPair([]byte("aaa"), []byte("v1")),
			NewPair([]byte("bbb"), []byte("v2")),
			NewPair([]byte("ccc"), []byte("v3")),
		})

		// WHEN
		slotNum, found := ln.SearchSlotNum([]byte("bbb"))

		// THEN
		assert.True(t, found)
		assert.Equal(t, 1, slotNum)
	})

	t.Run("存在しないキーの場合、挿入位置と false を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Pair{
			NewPair([]byte("aaa"), []byte("v1")),
			NewPair([]byte("ccc"), []byte("v2")),
		})

		// WHEN
		slotNum, found := ln.SearchSlotNum([]byte("bbb"))

		// THEN
		assert.False(t, found)
		assert.Equal(t, 1, slotNum)
	})

	t.Run("空のノードで検索した場合、(0, false) を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)

		// WHEN
		slotNum, found := ln.SearchSlotNum([]byte("aaa"))

		// THEN
		assert.False(t, found)
		assert.Equal(t, 0, slotNum)
	})

	t.Run("先頭より小さいキーの場合、挿入位置 0 と false を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Pair{
			NewPair([]byte("bbb"), []byte("v1")),
			NewPair([]byte("ccc"), []byte("v2")),
			NewPair([]byte("ddd"), []byte("v3")),
		})

		// WHEN
		slotNum, found := ln.SearchSlotNum([]byte("aaa"))

		// THEN
		assert.False(t, found)
		assert.Equal(t, 0, slotNum)
	})

	t.Run("末尾より大きいキーの場合、末尾の挿入位置と false を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Pair{
			NewPair([]byte("aaa"), []byte("v1")),
			NewPair([]byte("bbb"), []byte("v2")),
			NewPair([]byte("ccc"), []byte("v3")),
		})

		// WHEN
		slotNum, found := ln.SearchSlotNum([]byte("zzz"))

		// THEN
		assert.False(t, found)
		assert.Equal(t, 3, slotNum)
	})
}

func TestLeafNodePrevPageId(t *testing.T) {
	t.Run("初期化直後は nil を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)

		// WHEN
		prevPageId := ln.PrevPageId()

		// THEN
		assert.Nil(t, prevPageId)
	})

	t.Run("設定されたページ ID が取得できる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		pid := page.NewPageId(0, 42)
		ln.SetPrevPageId(&pid)

		// WHEN
		prevPageId := ln.PrevPageId()

		// THEN
		assert.NotNil(t, prevPageId)
		assert.Equal(t, pid, *prevPageId)
	})
}

func TestLeafNodeNextPageId(t *testing.T) {
	t.Run("初期化直後は nil を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)

		// WHEN
		nextPageId := ln.NextPageId()

		// THEN
		assert.Nil(t, nextPageId)
	})

	t.Run("設定されたページ ID が取得できる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		pid := page.NewPageId(0, 99)
		ln.SetNextPageId(&pid)

		// WHEN
		nextPageId := ln.NextPageId()

		// THEN
		assert.NotNil(t, nextPageId)
		assert.Equal(t, pid, *nextPageId)
	})
}

func TestLeafNodeSetPrevPageId(t *testing.T) {
	t.Run("nil を設定すると PrevPageId が nil に戻る", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		pid := page.NewPageId(0, 10)
		ln.SetPrevPageId(&pid)

		// WHEN
		ln.SetPrevPageId(nil)

		// THEN
		assert.Nil(t, ln.PrevPageId())
	})
}

func TestLeafNodeSetNextPageId(t *testing.T) {
	t.Run("nil を設定すると NextPageId が nil に戻る", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		pid := page.NewPageId(0, 10)
		ln.SetNextPageId(&pid)

		// WHEN
		ln.SetNextPageId(nil)

		// THEN
		assert.Nil(t, ln.NextPageId())
	})
}

func TestLeafNodeTransferAllFrom(t *testing.T) {
	t.Run("すべてのペアが転送元から自分に移動する", func(t *testing.T) {
		// GIVEN
		src := createTestLeafNode([]Pair{
			NewPair([]byte("key1"), []byte("val1")),
			NewPair([]byte("key2"), []byte("val2")),
		})
		dest := createTestLeafNode([]Pair{
			NewPair([]byte("key0"), []byte("val0")),
		})

		// WHEN
		dest.TransferAllFrom(src)

		// THEN
		assert.Equal(t, 0, src.NumPairs())
		assert.Equal(t, 3, dest.NumPairs())
		assert.Equal(t, []byte("key0"), dest.PairAt(0).Key)
		assert.Equal(t, []byte("key1"), dest.PairAt(1).Key)
		assert.Equal(t, []byte("key2"), dest.PairAt(2).Key)
	})

	t.Run("転送元が空の場合、転送先のデータが変わらない", func(t *testing.T) {
		// GIVEN
		src := createTestLeafNode(nil)
		dest := createTestLeafNode([]Pair{
			NewPair([]byte("key1"), []byte("val1")),
		})

		// WHEN
		dest.TransferAllFrom(src)

		// THEN
		assert.Equal(t, 0, src.NumPairs())
		assert.Equal(t, 1, dest.NumPairs())
		assert.Equal(t, []byte("key1"), dest.PairAt(0).Key)
	})
}

func TestLeafNodeIsHalfFull(t *testing.T) {
	t.Run("ペアが十分にある場合、true を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		bigValue := make([]byte, 1500)
		ln.Insert(0, NewPair([]byte("key1"), bigValue))
		ln.Insert(1, NewPair([]byte("key2"), bigValue))

		// WHEN
		result := ln.IsHalfFull()

		// THEN
		assert.True(t, result)
	})

	t.Run("ペアが少ない場合、false を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		ln.Insert(0, NewPair([]byte("k"), []byte("v")))

		// WHEN
		result := ln.IsHalfFull()

		// THEN
		assert.False(t, result)
	})
}

// テスト用のリーフノードを作成する
func createTestLeafNode(pairs []Pair) *LeafNode {
	data := directio.AlignedBlock(directio.BlockSize)
	ln := NewLeafNode(data)
	ln.Initialize()
	for i, pair := range pairs {
		if !ln.Insert(i, pair) {
			panic("failed to insert pair into leaf node")
		}
	}
	return ln
}

// リーフノード内のキーが昇順であることを検証するヘルパー
func assertKeysSorted(t *testing.T, ln *LeafNode) {
	t.Helper()
	for i := 1; i < ln.NumPairs(); i++ {
		prev := ln.PairAt(i - 1).Key
		curr := ln.PairAt(i).Key
		assert.True(t, bytes.Compare(prev, curr) < 0,
			"キーが昇順でない: index %d (%s) >= index %d (%s)", i-1, prev, i, curr)
	}
}
