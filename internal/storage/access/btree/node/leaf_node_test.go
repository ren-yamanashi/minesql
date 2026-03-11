package node

import (
	"bytes"
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
}

func TestLeafNodeDelete(t *testing.T) {
	t.Run("指定したスロット番号のペアが削除される", func(t *testing.T) {
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

func TestLeafNodeSplitInsert(t *testing.T) {
	t.Run("ノードが分割され、新旧両方にペアが分散する", func(t *testing.T) {
		// GIVEN: リーフノードを満杯にする
		ln := createTestLeafNode(nil)
		value := make([]byte, 200)
		i := 0
		for {
			key := []byte{byte('a' + i)}
			if !ln.Insert(i, NewPair(key, value)) {
				// 満杯になったので、この key を SplitInsert に渡す
				newLn := createTestLeafNode(nil)

				// WHEN
				minKey, err := ln.SplitInsert(newLn, NewPair(key, value))

				// THEN
				assert.NoError(t, err)
				assert.NotNil(t, minKey)
				assert.Greater(t, ln.NumPairs(), 0)
				assert.Greater(t, newLn.NumPairs(), 0)
				// 新旧合わせたペア数が元のペア数 + 1 (分割挿入した分)
				assert.Equal(t, i+1, ln.NumPairs()+newLn.NumPairs())
				return
			}
			i++
		}
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
}

func TestLeafNodeIsHalfFull(t *testing.T) {
	t.Run("ペアが十分にある場合、true を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		// 大きなペアを挿入して半分以上埋める
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

func TestLeafNodeCanLendPair(t *testing.T) {
	t.Run("ペアが 1 つしかない場合、false を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		bigValue := make([]byte, 1000)
		ln.Insert(0, NewPair([]byte("key1"), bigValue))

		// WHEN
		result := ln.CanLendPair()

		// THEN
		assert.False(t, result)
	})

	t.Run("貸した後も半分以上埋まっている場合、true を返す", func(t *testing.T) {
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
		result := ln.CanLendPair()

		// THEN
		assert.True(t, result)
	})

	t.Run("貸した後に半分を下回る場合、false を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		bigValue := make([]byte, 500)
		ln.Insert(0, NewPair([]byte("key1"), bigValue))
		ln.Insert(1, NewPair([]byte("key2"), bigValue))

		// WHEN
		result := ln.CanLendPair()

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
