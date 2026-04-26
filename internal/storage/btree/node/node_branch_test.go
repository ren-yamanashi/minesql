package node

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ncw/directio"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewBranch(t *testing.T) {
	t.Run("ノードタイプが BRANCH に設定される", func(t *testing.T) {
		// GIVEN
		data := directio.AlignedBlock(directio.BlockSize)

		// WHEN
		bn := NewBranch(data)

		// THEN
		assert.NotNil(t, bn)
		assert.True(t, bytes.Equal(data[0:8], NodeTypeBranch))
	})
}

func TestBranchBody(t *testing.T) {
	t.Run("ノードタイプヘッダーを除いたボディ部分が取得できる", func(t *testing.T) {
		// GIVEN
		data := directio.AlignedBlock(directio.BlockSize)
		bn := NewBranch(data)

		// WHEN
		body := bn.Body()

		// THEN
		assert.Equal(t, len(data)-headerSize, len(body))
	})
}

func TestBranchNumRecords(t *testing.T) {
	t.Run("Initialize 直後はレコード数が 1", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{NewRecord(nil, []byte("key1"), pageIdBytes(10))},
			page.NewPageId(0, 20),
		)

		// WHEN
		numRecords := bn.NumRecords()

		// THEN
		assert.Equal(t, 1, numRecords)
	})

	t.Run("挿入後のレコード数が正しく取得できる", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("key1"), pageIdBytes(10)),
				NewRecord(nil, []byte("key2"), pageIdBytes(20)),
				NewRecord(nil, []byte("key3"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN
		numRecords := bn.NumRecords()

		// THEN
		assert.Equal(t, 3, numRecords)
	})
}

func TestBranchRecordAt(t *testing.T) {
	t.Run("指定したスロット番号のレコードが取得できる", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("key1"), pageIdBytes(10)),
				NewRecord(nil, []byte("key2"), pageIdBytes(20)),
			},
			page.NewPageId(0, 30),
		)

		// WHEN
		record := bn.RecordAt(1)

		// THEN
		assert.Equal(t, []byte("key2"), record.KeyBytes())
		assert.Equal(t, pageIdBytes(20), record.NonKeyBytes())
	})
}

func TestBranchSearchSlotNum(t *testing.T) {
	t.Run("存在するキーの場合、スロット番号と true を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("aaa"), pageIdBytes(10)),
				NewRecord(nil, []byte("bbb"), pageIdBytes(20)),
				NewRecord(nil, []byte("ccc"), pageIdBytes(30)),
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
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("aaa"), pageIdBytes(10)),
				NewRecord(nil, []byte("ccc"), pageIdBytes(30)),
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
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("bbb"), pageIdBytes(10)),
				NewRecord(nil, []byte("ccc"), pageIdBytes(20)),
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
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("aaa"), pageIdBytes(10)),
				NewRecord(nil, []byte("bbb"), pageIdBytes(20)),
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

func TestBranchInsert(t *testing.T) {
	t.Run("レコードが正しく挿入できる", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{NewRecord(nil, []byte("aaa"), pageIdBytes(10))},
			page.NewPageId(0, 20),
		)

		// WHEN
		ok := bn.Insert(1, NewRecord(nil, []byte("bbb"), pageIdBytes(30)))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 2, bn.NumRecords())
		assert.Equal(t, []byte("bbb"), bn.RecordAt(1).KeyBytes())
	})

	t.Run("中間位置へのレコード挿入でスロットが正しくシフトされる", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("aaa"), pageIdBytes(10)),
				NewRecord(nil, []byte("ccc"), pageIdBytes(30)),
				NewRecord(nil, []byte("ddd"), pageIdBytes(40)),
			},
			page.NewPageId(0, 50),
		)

		// WHEN
		ok := bn.Insert(1, NewRecord(nil, []byte("bbb"), pageIdBytes(20)))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 4, bn.NumRecords())
		assert.Equal(t, []byte("aaa"), bn.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("bbb"), bn.RecordAt(1).KeyBytes())
		assert.Equal(t, []byte("ccc"), bn.RecordAt(2).KeyBytes())
		assert.Equal(t, []byte("ddd"), bn.RecordAt(3).KeyBytes())
	})

	t.Run("最大レコードサイズを超える場合、挿入に失敗する", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{NewRecord(nil, []byte("aaa"), pageIdBytes(10))},
			page.NewPageId(0, 20),
		)
		hugeKey := make([]byte, 4000)

		// WHEN
		ok := bn.Insert(1, NewRecord(nil, hugeKey, pageIdBytes(30)))

		// THEN
		assert.False(t, ok)
		assert.Equal(t, 1, bn.NumRecords())
	})

	t.Run("ページが満杯の場合、挿入に失敗し既存データが壊れない", func(t *testing.T) {
		// GIVEN: ノードをほぼ満杯にする
		bn := createTestBranchEmpty()
		bn.body.Initialize()
		value := pageIdBytes(1)
		inserted := 0
		for {
			key := fmt.Appendf(nil, "k%04d", inserted)
			if !bn.Insert(inserted, NewRecord(nil, key, value)) {
				break
			}
			inserted++
		}
		numBefore := bn.NumRecords()
		firstKeyBefore := make([]byte, len(bn.RecordAt(0).KeyBytes()))
		copy(firstKeyBefore, bn.RecordAt(0).KeyBytes())

		// WHEN: もう 1 つ挿入を試みる (満杯で入らなかったキーと同サイズ)
		overflowKey := fmt.Appendf(nil, "k%04d", inserted)
		ok := bn.Insert(0, NewRecord(nil, overflowKey, value))

		// THEN
		assert.False(t, ok)
		assert.Equal(t, numBefore, bn.NumRecords())
		assert.Equal(t, firstKeyBefore, bn.RecordAt(0).KeyBytes())
	})
}

func TestBranchInitialize(t *testing.T) {
	t.Run("初期化後にレコード数が 1 で正しいキーと子ページ ID が設定される", func(t *testing.T) {
		// GIVEN
		data := directio.AlignedBlock(directio.BlockSize)
		bn := NewBranch(data)
		leftChild := page.NewPageId(0, 10)
		rightChild := page.NewPageId(0, 20)

		// WHEN
		err := bn.Initialize([]byte("key1"), leftChild, rightChild)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, bn.NumRecords())
		assert.Equal(t, []byte("key1"), bn.RecordAt(0).KeyBytes())
		assert.Equal(t, leftChild, page.RestorePageIdFromBytes(bn.RecordAt(0).NonKeyBytes()))
		assert.Equal(t, rightChild, bn.RightChildPageId())
	})
}

func TestBranchSearchChildSlotNum(t *testing.T) {
	t.Run("キーが見つかった場合、slotNum + 1 を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("aaa"), pageIdBytes(10)),
				NewRecord(nil, []byte("bbb"), pageIdBytes(20)),
				NewRecord(nil, []byte("ccc"), pageIdBytes(30)),
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
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("aaa"), pageIdBytes(10)),
				NewRecord(nil, []byte("ccc"), pageIdBytes(30)),
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
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("bbb"), pageIdBytes(10)),
				NewRecord(nil, []byte("ccc"), pageIdBytes(20)),
			},
			page.NewPageId(0, 30),
		)

		// WHEN
		childSlotNum := bn.SearchChildSlotNum([]byte("aaa"))

		// THEN
		assert.Equal(t, 0, childSlotNum)
	})

	t.Run("すべてのキーより大きい場合、NumRecords を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("aaa"), pageIdBytes(10)),
				NewRecord(nil, []byte("bbb"), pageIdBytes(20)),
			},
			page.NewPageId(0, 30),
		)

		// WHEN
		childSlotNum := bn.SearchChildSlotNum([]byte("zzz"))

		// THEN
		assert.Equal(t, 2, childSlotNum)
	})
}

func TestBranchChildPageIdAt(t *testing.T) {
	t.Run("通常のスロット番号の場合、レコードの非キーフィールドからページ ID を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("key1"), pageIdBytes(10)),
				NewRecord(nil, []byte("key2"), pageIdBytes(20)),
			},
			page.NewPageId(0, 30),
		)

		// WHEN
		childPageId := bn.ChildPageIdAt(0)

		// THEN
		assert.Equal(t, page.NewPageId(0, 10), childPageId)
	})

	t.Run("スロット番号が NumRecords と等しい場合、右端の子ページ ID を返す", func(t *testing.T) {
		// GIVEN
		rightChild := page.NewPageId(0, 99)
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("key1"), pageIdBytes(10)),
				NewRecord(nil, []byte("key2"), pageIdBytes(20)),
			},
			rightChild,
		)

		// WHEN
		childPageId := bn.ChildPageIdAt(2)

		// THEN
		assert.Equal(t, rightChild, childPageId)
	})
}

func TestBranchSplitInsert(t *testing.T) {
	t.Run("昇順挿入で分割され、キー順序と minKey が正しい", func(t *testing.T) {
		// GIVEN: ブランチノードを昇順キーで満杯にする
		bn := createTestBranchEmpty()
		bn.body.Initialize()
		value := pageIdBytes(1)
		numInserted := 0
		for {
			key := fmt.Appendf(nil, "k%04d", numInserted)
			if !bn.Insert(numInserted, NewRecord(nil, key, value)) {
				break
			}
			numInserted++
		}
		overflowKey := fmt.Appendf(nil, "k%04d", numInserted)
		newBn := createTestBranchEmpty()

		// WHEN
		minKey, err := bn.SplitInsert(newBn, NewRecord(nil, overflowKey, value))

		// THEN
		assert.NoError(t, err)
		assert.Greater(t, bn.NumRecords(), 0)
		assert.Greater(t, newBn.NumRecords(), 0)
		assert.NotNil(t, minKey)
		assert.True(t, newBn.IsHalfFull())
		// fillRightChild で 1 レコードがセパレータとして昇格するため、合計は numInserted
		assert.Equal(t, numInserted, bn.NumRecords()+newBn.NumRecords())

		// minKey が新ノードの最大キーより大きく、旧ノードの最小キーより小さい
		newLastKey := newBn.RecordAt(newBn.NumRecords() - 1).KeyBytes()
		oldFirstKey := bn.RecordAt(0).KeyBytes()
		assert.True(t, bytes.Compare(newLastKey, minKey) < 0)
		assert.True(t, bytes.Compare(minKey, oldFirstKey) < 0)

		// 各ノード内のキーが昇順
		assertBranchKeysSorted(t, bn)
		assertBranchKeysSorted(t, newBn)

		// 新ノードの RightChildPageId が有効なページ ID である
		newRightChild := newBn.RightChildPageId()
		assert.NotEqual(t, page.PageId{}, newRightChild)
	})

	t.Run("既存の最小キーより小さいキーで分割できる", func(t *testing.T) {
		// GIVEN: ブランチノードを "b" 始まりのキーで満杯にする
		bn := createTestBranchEmpty()
		bn.body.Initialize()
		value := pageIdBytes(1)
		numInserted := 0
		for {
			key := fmt.Appendf(nil, "b%04d", numInserted)
			if !bn.Insert(numInserted, NewRecord(nil, key, value)) {
				break
			}
			numInserted++
		}
		smallKey := []byte("a0000")
		newBn := createTestBranchEmpty()

		// WHEN
		minKey, err := bn.SplitInsert(newBn, NewRecord(nil, smallKey, value))

		// THEN
		assert.NoError(t, err)
		assert.Greater(t, bn.NumRecords(), 0)
		assert.Greater(t, newBn.NumRecords(), 0)
		assert.NotNil(t, minKey)
		assert.True(t, newBn.IsHalfFull())
		assert.Equal(t, numInserted, bn.NumRecords()+newBn.NumRecords())

		// 各ノード内のキーが昇順
		assertBranchKeysSorted(t, bn)
		assertBranchKeysSorted(t, newBn)

		// minKey が新ノードの最大キーより大きく、旧ノードの最小キーより小さい
		newLastKey := newBn.RecordAt(newBn.NumRecords() - 1).KeyBytes()
		oldFirstKey := bn.RecordAt(0).KeyBytes()
		assert.True(t, bytes.Compare(newLastKey, minKey) < 0)
		assert.True(t, bytes.Compare(minKey, oldFirstKey) < 0)

		// 新ノードの RightChildPageId が有効なページ ID である
		newRightChild := newBn.RightChildPageId()
		assert.NotEqual(t, page.PageId{}, newRightChild)
	})

	t.Run("中間キーで分割される場合、キー順序が正しい", func(t *testing.T) {
		// GIVEN: ブランチノードを偶数キーで満杯にする
		bn := createTestBranchEmpty()
		bn.body.Initialize()
		value := pageIdBytes(1)
		numInserted := 0
		for {
			key := fmt.Appendf(nil, "a%04d", numInserted*2) // 偶数キー
			if !bn.Insert(numInserted, NewRecord(nil, key, value)) {
				break
			}
			numInserted++
		}
		// 中間付近に位置する奇数キーを挿入
		middleKey := fmt.Appendf(nil, "a%04d", numInserted)
		newBn := createTestBranchEmpty()

		// WHEN
		minKey, err := bn.SplitInsert(newBn, NewRecord(nil, middleKey, value))

		// THEN
		assert.NoError(t, err)
		assert.Greater(t, bn.NumRecords(), 0)
		assert.Greater(t, newBn.NumRecords(), 0)
		assert.NotNil(t, minKey)
		assert.True(t, newBn.IsHalfFull())
		assert.Equal(t, numInserted, bn.NumRecords()+newBn.NumRecords())

		// minKey が新ノードの最大キーより大きく、旧ノードの最小キーより小さい
		newLastKey := newBn.RecordAt(newBn.NumRecords() - 1).KeyBytes()
		oldFirstKey := bn.RecordAt(0).KeyBytes()
		assert.True(t, bytes.Compare(newLastKey, minKey) < 0)
		assert.True(t, bytes.Compare(minKey, oldFirstKey) < 0)

		// 各ノード内のキーが昇順
		assertBranchKeysSorted(t, bn)
		assertBranchKeysSorted(t, newBn)

		// 新ノードの RightChildPageId が有効なページ ID である
		newRightChild := newBn.RightChildPageId()
		assert.NotEqual(t, page.PageId{}, newRightChild)
	})
}

func TestBranchDelete(t *testing.T) {
	t.Run("中間レコードの削除が正しく動作する", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("key1"), pageIdBytes(10)),
				NewRecord(nil, []byte("key2"), pageIdBytes(20)),
				NewRecord(nil, []byte("key3"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN
		bn.Delete(1)

		// THEN
		assert.Equal(t, 2, bn.NumRecords())
		assert.Equal(t, []byte("key1"), bn.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("key3"), bn.RecordAt(1).KeyBytes())
	})

	t.Run("先頭レコードの削除が正しく動作する", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("key1"), pageIdBytes(10)),
				NewRecord(nil, []byte("key2"), pageIdBytes(20)),
				NewRecord(nil, []byte("key3"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN
		bn.Delete(0)

		// THEN
		assert.Equal(t, 2, bn.NumRecords())
		assert.Equal(t, []byte("key2"), bn.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("key3"), bn.RecordAt(1).KeyBytes())
	})

	t.Run("末尾レコードの削除が正しく動作する", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("key1"), pageIdBytes(10)),
				NewRecord(nil, []byte("key2"), pageIdBytes(20)),
				NewRecord(nil, []byte("key3"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN
		bn.Delete(2)

		// THEN
		assert.Equal(t, 2, bn.NumRecords())
		assert.Equal(t, []byte("key1"), bn.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("key2"), bn.RecordAt(1).KeyBytes())
	})
}

func TestBranchIsHalfFull(t *testing.T) {
	t.Run("レコードが十分にある場合、true を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchEmpty()
		bigKey := make([]byte, 1500)
		bn.body.Initialize()
		bn.Insert(0, NewRecord(nil, bigKey, pageIdBytes(10)))
		copy(bigKey, []byte("key2"))
		bn.Insert(1, NewRecord(nil, bigKey, pageIdBytes(20)))

		// WHEN
		result := bn.IsHalfFull()

		// THEN
		assert.True(t, result)
	})

	t.Run("レコードが少ない場合、false を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchEmpty()
		bn.body.Initialize()
		bn.Insert(0, NewRecord(nil, []byte("k"), pageIdBytes(10)))

		// WHEN
		result := bn.IsHalfFull()

		// THEN
		assert.False(t, result)
	})
}

func TestBranchCanTransferRecord(t *testing.T) {
	t.Run("レコードが 0 の場合、false を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchEmpty()
		bn.body.Initialize()

		// WHEN
		result := bn.CanTransferRecord(false)

		// THEN
		assert.False(t, result)
	})

	t.Run("レコードが 1 つしかない場合、false を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{NewRecord(nil, []byte("key1"), pageIdBytes(10))},
			page.NewPageId(0, 20),
		)

		// WHEN
		result := bn.CanTransferRecord(false)

		// THEN
		assert.False(t, result)
	})

	t.Run("左の兄弟に転送 (先頭レコードを転送) 後も半分以上埋まっている場合、true を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchEmpty()
		bn.body.Initialize()
		bigKey := make([]byte, 500)
		for i := range 6 {
			copy(bigKey, fmt.Appendf(nil, "key%d", i))
			bn.Insert(i, NewRecord(nil, bigKey, pageIdBytes(page.PageNumber(i*10))))
		}

		// WHEN
		result := bn.CanTransferRecord(false)

		// THEN
		assert.True(t, result)
	})

	t.Run("右の兄弟に転送 (末尾レコードを転送) 後も半分以上埋まっている場合、true を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchEmpty()
		bn.body.Initialize()
		bigKey := make([]byte, 500)
		for i := range 6 {
			copy(bigKey, fmt.Appendf(nil, "key%d", i))
			bn.Insert(i, NewRecord(nil, bigKey, pageIdBytes(page.PageNumber(i*10))))
		}

		// WHEN
		result := bn.CanTransferRecord(true)

		// THEN
		assert.True(t, result)
	})

	t.Run("転送後に半分を下回る場合、false を返す", func(t *testing.T) {
		// GIVEN
		bn := createTestBranchEmpty()
		bn.body.Initialize()
		bigKey := make([]byte, 500)
		copy(bigKey, []byte("key1"))
		bn.Insert(0, NewRecord(nil, bigKey, pageIdBytes(10)))
		copy(bigKey, []byte("key2"))
		bn.Insert(1, NewRecord(nil, bigKey, pageIdBytes(20)))

		// WHEN
		result := bn.CanTransferRecord(false)

		// THEN
		assert.False(t, result)
	})
}

func TestBranchUpdate(t *testing.T) {
	t.Run("指定したスロットのキーが更新される", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("aaa"), pageIdBytes(10)),
				NewRecord(nil, []byte("bbb"), pageIdBytes(20)),
				NewRecord(nil, []byte("ddd"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN: key "bbb" を同じ長さの "ccc" に更新
		ok := bn.Update(1, []byte("ccc"))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 3, bn.NumRecords())
		assert.Equal(t, []byte("ccc"), bn.RecordAt(1).KeyBytes())
	})

	t.Run("更新後もレコード数や他のキーに影響がない", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("aaa"), pageIdBytes(10)),
				NewRecord(nil, []byte("bbb"), pageIdBytes(20)),
			},
			page.NewPageId(0, 30),
		)

		// WHEN
		ok := bn.Update(0, []byte("aab"))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 2, bn.NumRecords())
		assert.Equal(t, []byte("aab"), bn.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("bbb"), bn.RecordAt(1).KeyBytes())
	})

	t.Run("異なる長さのキーで更新しても正しく動作する", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("aaa"), pageIdBytes(10)),
				NewRecord(nil, []byte("bbb"), pageIdBytes(20)),
				NewRecord(nil, []byte("ccc"), pageIdBytes(30)),
			},
			page.NewPageId(0, 40),
		)

		// WHEN: 3 バイトのキーを 6 バイトのキーに更新
		ok := bn.Update(1, []byte("bbbbbb"))

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 3, bn.NumRecords())
		assert.Equal(t, []byte("aaa"), bn.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("bbbbbb"), bn.RecordAt(1).KeyBytes())
		assert.Equal(t, []byte("ccc"), bn.RecordAt(2).KeyBytes())
		// 非キーフィールドも壊れていないことを確認
		assert.Equal(t, pageIdBytes(10), bn.RecordAt(0).NonKeyBytes())
		assert.Equal(t, pageIdBytes(30), bn.RecordAt(2).NonKeyBytes())
	})

	t.Run("空き容量が不足している場合、false を返す", func(t *testing.T) {
		// GIVEN: ノードをほぼ満杯にする
		bn := createTestBranchEmpty()
		bn.body.Initialize()
		value := pageIdBytes(1)
		inserted := 0
		for {
			key := fmt.Appendf(nil, "k%04d", inserted)
			if !bn.Insert(inserted, NewRecord(nil, key, value)) {
				break
			}
			inserted++
		}
		originalKey := make([]byte, len(bn.RecordAt(0).KeyBytes()))
		copy(originalKey, bn.RecordAt(0).KeyBytes())

		// WHEN: 非常に長いキーに更新を試みる
		hugeKey := make([]byte, 3000)
		ok := bn.Update(0, hugeKey)

		// THEN
		assert.False(t, ok)
		assert.Equal(t, originalKey, bn.RecordAt(0).KeyBytes())
	})
}

func TestBranchRightChildPageId(t *testing.T) {
	t.Run("右端の子ページ ID が正しく取得できる", func(t *testing.T) {
		// GIVEN
		rightChild := page.NewPageId(0, 99)
		bn := createTestBranch(
			[]Record{NewRecord(nil, []byte("key1"), pageIdBytes(10))},
			rightChild,
		)

		// WHEN
		result := bn.RightChildPageId()

		// THEN
		assert.Equal(t, rightChild, result)
	})
}

func TestBranchSetRightChildPageId(t *testing.T) {
	t.Run("右端の子ページ ID が正しく設定できる", func(t *testing.T) {
		// GIVEN
		bn := createTestBranch(
			[]Record{NewRecord(nil, []byte("key1"), pageIdBytes(10))},
			page.NewPageId(0, 40),
		)
		newRightChild := page.NewPageId(0, 200)

		// WHEN
		bn.SetRightChildPageId(newRightChild)

		// THEN
		assert.Equal(t, newRightChild, bn.RightChildPageId())
	})
}

func TestBranchTransferAllFrom(t *testing.T) {
	t.Run("すべてのレコードが転送元から自分に移動する", func(t *testing.T) {
		// GIVEN
		src := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("key3"), pageIdBytes(30)),
				NewRecord(nil, []byte("key4"), pageIdBytes(40)),
			},
			page.NewPageId(0, 50),
		)
		dest := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("key1"), pageIdBytes(10)),
			},
			page.NewPageId(0, 20),
		)

		// WHEN
		dest.TransferAllFrom(src)

		// THEN
		assert.Equal(t, 0, src.NumRecords())
		assert.Equal(t, 3, dest.NumRecords())
		assert.Equal(t, []byte("key1"), dest.RecordAt(0).KeyBytes())
		assert.Equal(t, []byte("key3"), dest.RecordAt(1).KeyBytes())
		assert.Equal(t, []byte("key4"), dest.RecordAt(2).KeyBytes())
	})

	t.Run("転送元が空の場合、転送先のデータが変わらない", func(t *testing.T) {
		// GIVEN
		src := createTestBranchEmpty()
		src.body.Initialize()
		dest := createTestBranch(
			[]Record{
				NewRecord(nil, []byte("key1"), pageIdBytes(10)),
			},
			page.NewPageId(0, 20),
		)

		// WHEN
		dest.TransferAllFrom(src)

		// THEN
		assert.Equal(t, 0, src.NumRecords())
		assert.Equal(t, 1, dest.NumRecords())
		assert.Equal(t, []byte("key1"), dest.RecordAt(0).KeyBytes())
	})
}

// テスト用のブランチノードを作成する (レコードあり)
func createTestBranch(records []Record, rightChildPageId page.PageId) *Branch {
	data := directio.AlignedBlock(directio.BlockSize)
	bn := NewBranch(data)

	if len(records) == 0 {
		panic("records must not be empty")
	}

	err := bn.Initialize(records[0].KeyBytes(), page.RestorePageIdFromBytes(records[0].NonKeyBytes()), rightChildPageId)
	if err != nil {
		panic("failed to initialize branch node")
	}

	for i, record := range records[1:] {
		if !bn.Insert(i+1, record) {
			panic("failed to insert record into branch node")
		}
	}

	return bn
}

// テスト用の空のブランチノードを作成する
func createTestBranchEmpty() *Branch {
	data := directio.AlignedBlock(directio.BlockSize)
	return NewBranch(data)
}

// ブランチノード内のキーが昇順であることを検証するヘルパー
func assertBranchKeysSorted(t *testing.T, bn *Branch) {
	t.Helper()
	for i := 1; i < bn.NumRecords(); i++ {
		prev := bn.RecordAt(i - 1).KeyBytes()
		curr := bn.RecordAt(i).KeyBytes()
		assert.True(t, bytes.Compare(prev, curr) < 0,
			"キーが昇順でない: index %d (%s) >= index %d (%s)", i-1, prev, i, curr)
	}
}

// page.PageId を ToBytes() するヘルパー
func pageIdBytes(pageNum page.PageNumber) []byte {
	pid := page.NewPageId(0, pageNum)
	return pid.ToBytes()
}
