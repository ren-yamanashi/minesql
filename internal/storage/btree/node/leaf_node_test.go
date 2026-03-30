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
	t.Run("初期化後にレコード数が 0、前後ページ ID が nil になる", func(t *testing.T) {
		// GIVEN
		data := directio.AlignedBlock(directio.BlockSize)
		ln := NewLeafNode(data)

		// WHEN
		ln.Initialize()

		// THEN
		assert.Equal(t, 0, ln.NumRecords())
		assert.Nil(t, ln.PrevPageId())
		assert.Nil(t, ln.NextPageId())
	})
}

func TestLeafNodeInsert(t *testing.T) {
	t.Run("レコードが正しく挿入できる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)

		// WHEN
		ok := ln.Insert(0, NewRecord(nil, []byte("key1"), []byte("val1")))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 1, ln.NumRecords())
		assert.Equal(t, []byte("key1"), ln.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("val1"), ln.RecordAt(0).NonKeyBytes())
	})

	t.Run("中間位置へのレコード挿入でスロットが正しくシフトされる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Record{
			NewRecord(nil, []byte("aaa"), []byte("v1")),
			NewRecord(nil, []byte("ccc"), []byte("v3")),
			NewRecord(nil, []byte("ddd"), []byte("v4")),
		})

		// WHEN
		ok := ln.Insert(1, NewRecord(nil, []byte("bbb"), []byte("v2")))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 4, ln.NumRecords())
		assert.Equal(t, []byte("aaa"), ln.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("bbb"), ln.RecordAt(1).KeyBytes())
		assert.Equal(t, []byte("ccc"), ln.RecordAt(2).KeyBytes())
		assert.Equal(t, []byte("ddd"), ln.RecordAt(3).KeyBytes())
	})

	t.Run("最大レコードサイズを超える場合、挿入に失敗する", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		hugeValue := make([]byte, 4000)

		// WHEN
		ok := ln.Insert(0, NewRecord(nil, []byte("key"), hugeValue))

		// THEN
		assert.False(t, ok)
		assert.Equal(t, 0, ln.NumRecords())
	})

	t.Run("ページが満杯の場合、挿入に失敗し既存データが壊れない", func(t *testing.T) {
		// GIVEN: ノードをほぼ満杯にする
		ln := createTestLeafNode(nil)
		value := make([]byte, 200)
		inserted := 0
		for {
			key := fmt.Appendf(nil, "k%02d", inserted)
			if !ln.Insert(inserted, NewRecord(nil, key, value)) {
				break
			}
			inserted++
		}
		numBefore := ln.NumRecords()
		firstKeyBefore := make([]byte, len(ln.RecordAt(0).KeyBytes()))
		copy(firstKeyBefore, ln.RecordAt(0).KeyBytes())

		// WHEN: もう 1 つ挿入を試みる (小さいレコードだが容量不足)
		ok := ln.Insert(0, NewRecord(nil, []byte("new"), value))

		// THEN
		assert.False(t, ok)
		assert.Equal(t, numBefore, ln.NumRecords())
		assert.Equal(t, firstKeyBefore, ln.RecordAt(0).KeyBytes())
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
			if !ln.Insert(numInserted, NewRecord(nil, key, value)) {
				break
			}
			numInserted++
		}
		overflowKey := fmt.Appendf(nil, "k%02d", numInserted)
		newLn := createTestLeafNode(nil)

		// WHEN
		minKey, err := ln.SplitInsert(newLn, NewRecord(nil, overflowKey, value))

		// THEN
		assert.NoError(t, err)
		assert.Greater(t, ln.NumRecords(), 0)
		assert.Greater(t, newLn.NumRecords(), 0)
		assert.Equal(t, numInserted+1, ln.NumRecords()+newLn.NumRecords())

		// minKey が新ノードの先頭キーと一致する
		assert.Equal(t, newLn.RecordAt(0).KeyBytes(), minKey)

		// 新ノードの全キー < 旧ノードの全キー
		newLastKey := newLn.RecordAt(newLn.NumRecords() - 1).KeyBytes()
		oldFirstKey := ln.RecordAt(0).KeyBytes()
		assert.True(t, bytes.Compare(newLastKey, oldFirstKey) < 0)

		// 新ノードが半分以上埋まっている
		assert.True(t, newLn.IsHalfFull())

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
			if !ln.Insert(numInserted, NewRecord(nil, key, value)) {
				break
			}
			numInserted++
		}
		// 既存の最小キーより小さいキーで SplitInsert
		smallKey := []byte("a00")
		newLn := createTestLeafNode(nil)

		// WHEN
		minKey, err := ln.SplitInsert(newLn, NewRecord(nil, smallKey, value))

		// THEN
		assert.NoError(t, err)
		assert.Greater(t, ln.NumRecords(), 0)
		assert.Greater(t, newLn.NumRecords(), 0)
		assert.NotNil(t, minKey)
		assert.Equal(t, newLn.RecordAt(0).KeyBytes(), minKey)

		// 新ノードが半分以上埋まっている
		assert.True(t, newLn.IsHalfFull())

		// 各ノード内のキーが昇順
		assertKeysSorted(t, ln)
		assertKeysSorted(t, newLn)

		// 新ノードの全キー < 旧ノードの全キー
		newLastKey := newLn.RecordAt(newLn.NumRecords() - 1).KeyBytes()
		oldFirstKey := ln.RecordAt(0).KeyBytes()
		assert.True(t, bytes.Compare(newLastKey, oldFirstKey) < 0)
	})

	t.Run("中間キーで分割される場合、キー順序が正しい", func(t *testing.T) {
		// GIVEN: リーフノードを偶数キーで満杯にする
		ln := createTestLeafNode(nil)
		value := make([]byte, 200)
		numInserted := 0
		for {
			key := fmt.Appendf(nil, "a%04d", numInserted*2) // 偶数キー (a0000, a0002, a0004, ...)
			if !ln.Insert(numInserted, NewRecord(nil, key, value)) {
				break
			}
			numInserted++
		}
		// 中間付近に位置する奇数キーを挿入
		middleKey := fmt.Appendf(nil, "a%04d", numInserted)
		newLn := createTestLeafNode(nil)

		// WHEN
		minKey, err := ln.SplitInsert(newLn, NewRecord(nil, middleKey, value))

		// THEN
		assert.NoError(t, err)
		assert.Greater(t, ln.NumRecords(), 0)
		assert.Greater(t, newLn.NumRecords(), 0)
		assert.Equal(t, numInserted+1, ln.NumRecords()+newLn.NumRecords())
		assert.Equal(t, newLn.RecordAt(0).KeyBytes(), minKey)

		// 新ノードの全キー < 旧ノードの全キー
		newLastKey := newLn.RecordAt(newLn.NumRecords() - 1).KeyBytes()
		oldFirstKey := ln.RecordAt(0).KeyBytes()
		assert.True(t, bytes.Compare(newLastKey, oldFirstKey) < 0)

		// 各ノード内のキーが昇順
		assertKeysSorted(t, ln)
		assertKeysSorted(t, newLn)
	})
}

func TestLeafNodeDelete(t *testing.T) {
	t.Run("中間レコードの削除が正しく動作する", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Record{
			NewRecord(nil, []byte("key1"), []byte("val1")),
			NewRecord(nil, []byte("key2"), []byte("val2")),
			NewRecord(nil, []byte("key3"), []byte("val3")),
		})

		// WHEN
		ln.Delete(1)

		// THEN
		assert.Equal(t, 2, ln.NumRecords())
		assert.Equal(t, []byte("key1"), ln.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("key3"), ln.RecordAt(1).KeyBytes())
	})

	t.Run("先頭レコードの削除が正しく動作する", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Record{
			NewRecord(nil, []byte("key1"), []byte("val1")),
			NewRecord(nil, []byte("key2"), []byte("val2")),
			NewRecord(nil, []byte("key3"), []byte("val3")),
		})

		// WHEN
		ln.Delete(0)

		// THEN
		assert.Equal(t, 2, ln.NumRecords())
		assert.Equal(t, []byte("key2"), ln.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("key3"), ln.RecordAt(1).KeyBytes())
	})

	t.Run("末尾レコードの削除が正しく動作する", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Record{
			NewRecord(nil, []byte("key1"), []byte("val1")),
			NewRecord(nil, []byte("key2"), []byte("val2")),
			NewRecord(nil, []byte("key3"), []byte("val3")),
		})

		// WHEN
		ln.Delete(2)

		// THEN
		assert.Equal(t, 2, ln.NumRecords())
		assert.Equal(t, []byte("key1"), ln.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("key2"), ln.RecordAt(1).KeyBytes())
	})

	t.Run("唯一のレコードを削除するとレコード数が 0 になる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Record{
			NewRecord(nil, []byte("key1"), []byte("val1")),
		})

		// WHEN
		ln.Delete(0)

		// THEN
		assert.Equal(t, 0, ln.NumRecords())
	})
}

func TestLeafNodeUpdate(t *testing.T) {
	t.Run("非キーフィールドを更新できる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Record{
			NewRecord(nil, []byte("key1"), []byte("val1")),
			NewRecord(nil, []byte("key2"), []byte("val2")),
			NewRecord(nil, []byte("key3"), []byte("val3")),
		})

		// WHEN
		ok := ln.Update(1, NewRecord(nil, []byte("key2"), []byte("updated")))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 3, ln.NumRecords())
		assert.Equal(t, []byte("key2"), ln.RecordAt(1).KeyBytes())
		assert.Equal(t, []byte("updated"), ln.RecordAt(1).NonKeyBytes())
	})

	t.Run("非キーフィールドを短い値に更新できる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Record{
			NewRecord(nil, []byte("key1"), []byte("long_value_here")),
		})

		// WHEN
		ok := ln.Update(0, NewRecord(nil, []byte("key1"), []byte("short")))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte("key1"), ln.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("short"), ln.RecordAt(0).NonKeyBytes())
	})

	t.Run("非キーフィールドを長い値に更新できる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Record{
			NewRecord(nil, []byte("key1"), []byte("short")),
		})

		// WHEN
		ok := ln.Update(0, NewRecord(nil, []byte("key1"), []byte("much_longer_value")))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte("key1"), ln.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("much_longer_value"), ln.RecordAt(0).NonKeyBytes())
	})

	t.Run("更新後も他のレコードが壊れない", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Record{
			NewRecord(nil, []byte("key1"), []byte("val1")),
			NewRecord(nil, []byte("key2"), []byte("val2")),
			NewRecord(nil, []byte("key3"), []byte("val3")),
		})

		// WHEN
		ok := ln.Update(1, NewRecord(nil, []byte("key2"), []byte("new_longer_value")))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte("key1"), ln.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("val1"), ln.RecordAt(0).NonKeyBytes())
		assert.Equal(t, []byte("key3"), ln.RecordAt(2).KeyBytes())
		assert.Equal(t, []byte("val3"), ln.RecordAt(2).NonKeyBytes())
	})

	t.Run("空き容量が不足している場合、false を返す", func(t *testing.T) {
		// GIVEN: ノードをほぼ満杯にする
		ln := createTestLeafNode(nil)
		value := make([]byte, 200)
		inserted := 0
		for {
			key := fmt.Appendf(nil, "k%02d", inserted)
			if !ln.Insert(inserted, NewRecord(nil, key, value)) {
				break
			}
			inserted++
		}
		originalValue := make([]byte, len(ln.RecordAt(0).NonKeyBytes()))
		copy(originalValue, ln.RecordAt(0).NonKeyBytes())

		// WHEN: 非常に大きな値に更新を試みる
		hugeValue := make([]byte, 3000)
		ok := ln.Update(0, NewRecord(nil, ln.RecordAt(0).KeyBytes(), hugeValue))

		// THEN
		assert.False(t, ok)
	})
}

func TestLeafNodeCanTransferRecord(t *testing.T) {
	t.Run("レコードが 0 の場合、false を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)

		// WHEN
		result := ln.CanTransferRecord(false)

		// THEN
		assert.False(t, result)
	})

	t.Run("レコードが 1 つしかない場合、false を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		bigValue := make([]byte, 1000)
		ln.Insert(0, NewRecord(nil, []byte("key1"), bigValue))

		// WHEN
		result := ln.CanTransferRecord(false)

		// THEN
		assert.False(t, result)
	})

	t.Run("左の兄弟に転送 (先頭レコードを転送) 後も半分以上埋まっている場合、true を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		bigValue := make([]byte, 500)
		ln.Insert(0, NewRecord(nil, []byte("key1"), bigValue))
		ln.Insert(1, NewRecord(nil, []byte("key2"), bigValue))
		ln.Insert(2, NewRecord(nil, []byte("key3"), bigValue))
		ln.Insert(3, NewRecord(nil, []byte("key4"), bigValue))
		ln.Insert(4, NewRecord(nil, []byte("key5"), bigValue))
		ln.Insert(5, NewRecord(nil, []byte("key6"), bigValue))

		// WHEN
		result := ln.CanTransferRecord(false)

		// THEN
		assert.True(t, result)
	})

	t.Run("右の兄弟に転送 (末尾レコードを転送) 後も半分以上埋まっている場合、true を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		bigValue := make([]byte, 500)
		ln.Insert(0, NewRecord(nil, []byte("key1"), bigValue))
		ln.Insert(1, NewRecord(nil, []byte("key2"), bigValue))
		ln.Insert(2, NewRecord(nil, []byte("key3"), bigValue))
		ln.Insert(3, NewRecord(nil, []byte("key4"), bigValue))
		ln.Insert(4, NewRecord(nil, []byte("key5"), bigValue))
		ln.Insert(5, NewRecord(nil, []byte("key6"), bigValue))

		// WHEN
		result := ln.CanTransferRecord(true)

		// THEN
		assert.True(t, result)
	})

	t.Run("転送後に半分を下回る場合、false を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		bigValue := make([]byte, 500)
		ln.Insert(0, NewRecord(nil, []byte("key1"), bigValue))
		ln.Insert(1, NewRecord(nil, []byte("key2"), bigValue))

		// WHEN
		result := ln.CanTransferRecord(false)

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

func TestLeafNodeNumRecords(t *testing.T) {
	t.Run("初期化直後はレコード数が 0", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)

		// WHEN
		numRecords := ln.NumRecords()

		// THEN
		assert.Equal(t, 0, numRecords)
	})

	t.Run("挿入後のレコード数が正しく取得できる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Record{
			NewRecord(nil, []byte("key1"), []byte("val1")),
			NewRecord(nil, []byte("key2"), []byte("val2")),
		})

		// WHEN
		numRecords := ln.NumRecords()

		// THEN
		assert.Equal(t, 2, numRecords)
	})
}

func TestLeafNodeRecordAt(t *testing.T) {
	t.Run("指定したスロット番号のレコードが取得できる", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Record{
			NewRecord(nil, []byte("key1"), []byte("val1")),
			NewRecord(nil, []byte("key2"), []byte("val2")),
		})

		// WHEN
		record := ln.RecordAt(1)

		// THEN
		assert.Equal(t, []byte("key2"), record.KeyBytes())
		assert.Equal(t, []byte("val2"), record.NonKeyBytes())
	})
}

func TestLeafNodeSearchSlotNum(t *testing.T) {
	t.Run("存在するキーの場合、スロット番号と true を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Record{
			NewRecord(nil, []byte("aaa"), []byte("v1")),
			NewRecord(nil, []byte("bbb"), []byte("v2")),
			NewRecord(nil, []byte("ccc"), []byte("v3")),
		})

		// WHEN
		slotNum, found := ln.SearchSlotNum([]byte("bbb"))

		// THEN
		assert.True(t, found)
		assert.Equal(t, 1, slotNum)
	})

	t.Run("存在しないキーの場合、挿入位置と false を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Record{
			NewRecord(nil, []byte("aaa"), []byte("v1")),
			NewRecord(nil, []byte("ccc"), []byte("v2")),
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
		ln := createTestLeafNode([]Record{
			NewRecord(nil, []byte("bbb"), []byte("v1")),
			NewRecord(nil, []byte("ccc"), []byte("v2")),
			NewRecord(nil, []byte("ddd"), []byte("v3")),
		})

		// WHEN
		slotNum, found := ln.SearchSlotNum([]byte("aaa"))

		// THEN
		assert.False(t, found)
		assert.Equal(t, 0, slotNum)
	})

	t.Run("末尾より大きいキーの場合、末尾の挿入位置と false を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode([]Record{
			NewRecord(nil, []byte("aaa"), []byte("v1")),
			NewRecord(nil, []byte("bbb"), []byte("v2")),
			NewRecord(nil, []byte("ccc"), []byte("v3")),
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

func TestLeafNodePageIdIndependence(t *testing.T) {
	t.Run("SetPrevPageId は NextPageId に影響を与えない", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		nextPid := page.NewPageId(0, 99)
		ln.SetNextPageId(&nextPid)

		// WHEN
		prevPid := page.NewPageId(0, 42)
		ln.SetPrevPageId(&prevPid)

		// THEN
		assert.Equal(t, nextPid, *ln.NextPageId())
		assert.Equal(t, prevPid, *ln.PrevPageId())
	})

	t.Run("SetNextPageId は PrevPageId に影響を与えない", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		prevPid := page.NewPageId(0, 42)
		ln.SetPrevPageId(&prevPid)

		// WHEN
		nextPid := page.NewPageId(0, 99)
		ln.SetNextPageId(&nextPid)

		// THEN
		assert.Equal(t, prevPid, *ln.PrevPageId())
		assert.Equal(t, nextPid, *ln.NextPageId())
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
	t.Run("すべてのレコードが転送元から自分に移動する", func(t *testing.T) {
		// GIVEN
		src := createTestLeafNode([]Record{
			NewRecord(nil, []byte("key1"), []byte("val1")),
			NewRecord(nil, []byte("key2"), []byte("val2")),
		})
		dest := createTestLeafNode([]Record{
			NewRecord(nil, []byte("key0"), []byte("val0")),
		})

		// WHEN
		ok := dest.TransferAllFrom(src)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 0, src.NumRecords())
		assert.Equal(t, 3, dest.NumRecords())
		assert.Equal(t, []byte("key0"), dest.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("key1"), dest.RecordAt(1).KeyBytes())
		assert.Equal(t, []byte("key2"), dest.RecordAt(2).KeyBytes())
	})

	t.Run("転送元が空の場合、転送先のデータが変わらない", func(t *testing.T) {
		// GIVEN
		src := createTestLeafNode(nil)
		dest := createTestLeafNode([]Record{
			NewRecord(nil, []byte("key1"), []byte("val1")),
		})

		// WHEN
		ok := dest.TransferAllFrom(src)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 0, src.NumRecords())
		assert.Equal(t, 1, dest.NumRecords())
		assert.Equal(t, []byte("key1"), dest.RecordAt(0).KeyBytes())
	})

	t.Run("転送先の空き容量が不足している場合、false を返す", func(t *testing.T) {
		// GIVEN: 転送元と転送先をそれぞれほぼ満杯にする
		src := createTestLeafNode(nil)
		dest := createTestLeafNode(nil)
		bigValue := make([]byte, 500)
		for i := 0; ; i++ {
			key := fmt.Appendf(nil, "s%02d", i)
			if !src.Insert(i, NewRecord(nil, key, bigValue)) {
				break
			}
		}
		for i := 0; ; i++ {
			key := fmt.Appendf(nil, "d%02d", i)
			if !dest.Insert(i, NewRecord(nil, key, bigValue)) {
				break
			}
		}
		srcNumBefore := src.NumRecords()

		// WHEN
		ok := dest.TransferAllFrom(src)

		// THEN
		assert.False(t, ok)
		assert.Equal(t, srcNumBefore, src.NumRecords())
	})
}

func TestLeafNodeIsHalfFull(t *testing.T) {
	t.Run("レコードが十分にある場合、true を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		bigValue := make([]byte, 1500)
		ln.Insert(0, NewRecord(nil, []byte("key1"), bigValue))
		ln.Insert(1, NewRecord(nil, []byte("key2"), bigValue))

		// WHEN
		result := ln.IsHalfFull()

		// THEN
		assert.True(t, result)
	})

	t.Run("レコードが少ない場合、false を返す", func(t *testing.T) {
		// GIVEN
		ln := createTestLeafNode(nil)
		ln.Insert(0, NewRecord(nil, []byte("k"), []byte("v")))

		// WHEN
		result := ln.IsHalfFull()

		// THEN
		assert.False(t, result)
	})
}

// テスト用のリーフノードを作成する
func createTestLeafNode(records []Record) *LeafNode {
	data := directio.AlignedBlock(directio.BlockSize)
	ln := NewLeafNode(data)
	ln.Initialize()
	for i, record := range records {
		if !ln.Insert(i, record) {
			panic("failed to insert record into leaf node")
		}
	}
	return ln
}

// リーフノード内のキーが昇順であることを検証するヘルパー
func assertKeysSorted(t *testing.T, ln *LeafNode) {
	t.Helper()
	for i := 1; i < ln.NumRecords(); i++ {
		prev := ln.RecordAt(i - 1).KeyBytes()
		curr := ln.RecordAt(i).KeyBytes()
		assert.True(t, bytes.Compare(prev, curr) < 0,
			"キーが昇順でない: index %d (%s) >= index %d (%s)", i-1, prev, i, curr)
	}
}
