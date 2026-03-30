package btree

import (
	"fmt"
	"minesql/internal/storage/btree/node"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/file"
	"minesql/internal/storage/page"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateBTree(t *testing.T) {
	t.Run("B+Tree が作成され、空の状態で検索できる", func(t *testing.T) {
		// GIVEN & WHEN
		bt, bp := setupBTree(t)

		// THEN
		records := bt.collectAllRecords(bp)
		assert.Equal(t, 0, len(records))
	})
}

func TestSearch(t *testing.T) {
	t.Run("SearchModeStart で空のツリーを検索すると結果が空になる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)

		// WHEN
		iter, err := bt.Search(bp, SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		_, ok := iter.Get()
		assert.False(t, ok)
	})

	t.Run("SearchModeStart で先頭のレコードが取得できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")

		// WHEN
		iter, err := bt.Search(bp, SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		record, ok := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, "aaa", string(record.KeyBytes()))
	})

	t.Run("SearchModeStart で分割が発生した B+Tree の全レコードを走査できる", func(t *testing.T) {
		// GIVEN: 多数のレコードを挿入してノード分割を発生させる
		bt, bp := setupBTree(t)
		numRecords := 100
		for i := range numRecords {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bp, key, val)
		}

		// WHEN: SearchModeStart で Next() を繰り返し呼ぶ
		iter, err := bt.Search(bp, SearchModeStart{})
		assert.NoError(t, err)
		var records []node.Record
		for {
			record, ok, err := iter.Next(bp)
			assert.NoError(t, err)
			if !ok {
				break
			}
			keyCopy := make([]byte, len(record.KeyBytes()))
			copy(keyCopy, record.KeyBytes())
			records = append(records, node.NewRecord(nil, keyCopy, nil))
		}

		// THEN: 全レコードが昇順で取得でき、最後に ok=false が返る
		assert.Equal(t, numRecords, len(records))
		for i, record := range records {
			expectedKey := fmt.Sprintf("key%03d", i)
			assert.Equal(t, expectedKey, string(record.KeyBytes()))
		}
	})

	t.Run("SearchModeKey で指定したキーの位置からイテレータが開始される", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")

		// WHEN
		iter, err := bt.Search(bp, SearchModeKey{Key: []byte("bbb")})

		// THEN
		assert.NoError(t, err)
		record, ok := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, "bbb", string(record.KeyBytes()))
	})

	t.Run("SearchModeKey で存在しないキーを検索すると挿入位置から開始される", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "ccc", "v3")
		bt.mustInsert(bp, "eee", "v5")

		// WHEN: "bbb" は存在しないが、"ccc" の位置から開始される
		iter, err := bt.Search(bp, SearchModeKey{Key: []byte("bbb")})

		// THEN
		assert.NoError(t, err)
		record, ok := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, "ccc", string(record.KeyBytes()))
	})

	t.Run("SearchModeKey で最大キーより大きいキーを検索すると結果が空になる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")

		// WHEN
		iter, err := bt.Search(bp, SearchModeKey{Key: []byte("zzz")})

		// THEN
		assert.NoError(t, err)
		_, ok := iter.Get()
		assert.False(t, ok)
	})

	t.Run("SearchModeKey で削除済みキーを検索すると次のキーから開始される", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")
		err := bt.Delete(bp, []byte("bbb"))
		assert.NoError(t, err)

		// WHEN: 削除済みの "bbb" で検索
		iter, err := bt.Search(bp, SearchModeKey{Key: []byte("bbb")})

		// THEN: 次のキー "ccc" から開始される
		assert.NoError(t, err)
		record, ok := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, "ccc", string(record.KeyBytes()))
	})

	t.Run("分割が発生した B+Tree で SearchModeKey が正しく動作する", func(t *testing.T) {
		// GIVEN: 多数のレコードを挿入してノード分割を発生させる
		bt, bp := setupBTree(t)
		numRecords := 100
		for i := range numRecords {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bp, key, val)
		}

		// WHEN: 中間のキーで検索
		iter, err := bt.Search(bp, SearchModeKey{Key: []byte("key050")})

		// THEN: key050 から順に取得できる
		assert.NoError(t, err)
		record, ok, err := iter.Next(bp)
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "key050", string(record.KeyBytes()))

		// 次のレコードも正しく取得できる
		record, ok, err = iter.Next(bp)
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "key051", string(record.KeyBytes()))
	})
}

func TestInsert(t *testing.T) {
	t.Run("1 つのレコードを挿入して検索できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)

		// WHEN
		err := bt.Insert(bp, node.NewRecord(nil, []byte("key1"), []byte("val1")))

		// THEN
		assert.NoError(t, err)
		records := bt.collectAllRecords(bp)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, "key1", string(records[0].KeyBytes()))
		assert.Equal(t, "val1", string(records[0].NonKeyBytes()))
	})

	t.Run("重複キーを挿入するとエラーが返る", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "key1", "val1")

		// WHEN
		err := bt.Insert(bp, node.NewRecord(nil, []byte("key1"), []byte("val2")))

		// THEN
		assert.ErrorIs(t, err, ErrDuplicateKey)
	})

	t.Run("挿入順に関わらずキーが昇順でソートされる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)

		// WHEN: 降順に挿入
		bt.mustInsert(bp, "ccc", "v3")
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")

		// THEN: 昇順で取得できる
		records := bt.collectAllRecords(bp)
		assert.Equal(t, 3, len(records))
		assert.Equal(t, "aaa", string(records[0].KeyBytes()))
		assert.Equal(t, "bbb", string(records[1].KeyBytes()))
		assert.Equal(t, "ccc", string(records[2].KeyBytes()))
	})

	t.Run("降順に多数のレコードを挿入してもすべて取得できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)

		// WHEN: 降順に挿入
		numRecords := 100
		for i := numRecords - 1; i >= 0; i-- {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bp, key, val)
		}

		// THEN: 全レコードが昇順で取得できる
		records := bt.collectAllRecords(bp)
		assert.Equal(t, numRecords, len(records))
		for i, record := range records {
			expectedKey := fmt.Sprintf("key%03d", i)
			expectedVal := fmt.Sprintf("val%03d", i)
			assert.Equal(t, expectedKey, string(record.KeyBytes()))
			assert.Equal(t, expectedVal, string(record.NonKeyBytes()))
		}
	})

	t.Run("分割後に重複キーを挿入するとエラーが返る", func(t *testing.T) {
		// GIVEN: 多数のレコードを挿入してノード分割を発生させる
		bt, bp := setupBTree(t)
		numRecords := 100
		for i := range numRecords {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bp, key, val)
		}

		// WHEN: 既存のキーで挿入を試みる
		err := bt.Insert(bp, node.NewRecord(nil, []byte("key050"), []byte("dup")))

		// THEN
		assert.ErrorIs(t, err, ErrDuplicateKey)
	})

	t.Run("多数のレコードを挿入してルート分割が発生しても全レコードが取得できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)

		// WHEN: 多数のレコードを挿入してノード分割を発生させる
		numRecords := 100
		for i := range numRecords {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bp, key, val)
		}

		// THEN: 全レコードが昇順で取得できる
		records := bt.collectAllRecords(bp)
		assert.Equal(t, numRecords, len(records))
		for i, record := range records {
			expectedKey := fmt.Sprintf("key%03d", i)
			expectedVal := fmt.Sprintf("val%03d", i)
			assert.Equal(t, expectedKey, string(record.KeyBytes()))
			assert.Equal(t, expectedVal, string(record.NonKeyBytes()))
		}
	})
}

func TestDelete(t *testing.T) {
	t.Run("リーフノードのみの B+Tree からレコードを削除できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "key1", "val1")
		bt.mustInsert(bp, "key2", "val2")
		bt.mustInsert(bp, "key3", "val3")

		// WHEN
		err := bt.Delete(bp, []byte("key2"))

		// THEN
		assert.NoError(t, err)
		records := bt.collectAllRecords(bp)
		assert.Equal(t, 2, len(records))
		assert.Equal(t, "key1", string(records[0].KeyBytes()))
		assert.Equal(t, "key3", string(records[1].KeyBytes()))
	})

	t.Run("存在しないキーを削除するとエラーが返る", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "key1", "val1")

		// WHEN
		err := bt.Delete(bp, []byte("nonexistent"))

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("すべてのレコードを削除できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "key1", "val1")
		bt.mustInsert(bp, "key2", "val2")

		// WHEN
		err1 := bt.Delete(bp, []byte("key1"))
		err2 := bt.Delete(bp, []byte("key2"))

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		records := bt.collectAllRecords(bp)
		assert.Equal(t, 0, len(records))
	})

	t.Run("分割が発生した B+Tree から削除できる", func(t *testing.T) {
		// GIVEN: 多数のレコードを挿入してノード分割を発生させる
		bt, bp := setupBTree(t)
		numRecords := 100
		for i := range numRecords {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bp, key, val)
		}

		// WHEN: 一部のレコードを削除
		for i := 0; i < numRecords; i += 2 {
			key := fmt.Sprintf("key%03d", i)
			err := bt.Delete(bp, []byte(key))
			assert.NoError(t, err)
		}

		// THEN: 残りのレコードが正しく取得できる
		records := bt.collectAllRecords(bp)
		assert.Equal(t, numRecords/2, len(records))
		for i, record := range records {
			expectedKey := fmt.Sprintf("key%03d", i*2+1)
			assert.Equal(t, expectedKey, string(record.KeyBytes()))
		}
	})

	t.Run("すべてのレコードを順次削除しても B+Tree が壊れない", func(t *testing.T) {
		// GIVEN: 多数のレコードを挿入 (200 件でリーフマージが発生する)
		bt, bp := setupBTree(t)
		numRecords := 200
		for i := range numRecords {
			key := fmt.Sprintf("key%04d", i)
			val := fmt.Sprintf("val%04d", i)
			bt.mustInsert(bp, key, val)
		}

		// WHEN: 全レコードを先頭から順に削除
		for i := range numRecords {
			key := fmt.Sprintf("key%04d", i)
			err := bt.Delete(bp, []byte(key))
			assert.NoError(t, err)
		}

		// THEN: B+Tree が空になる
		records := bt.collectAllRecords(bp)
		assert.Equal(t, 0, len(records))
	})

	t.Run("末尾から逆順に全レコードを削除しても B+Tree が壊れない", func(t *testing.T) {
		// GIVEN: 多数のレコードを挿入
		bt, bp := setupBTree(t)
		numRecords := 200
		for i := range numRecords {
			key := fmt.Sprintf("key%04d", i)
			val := fmt.Sprintf("val%04d", i)
			bt.mustInsert(bp, key, val)
		}

		// WHEN: 全レコードを末尾から逆順に削除
		for i := numRecords - 1; i >= 0; i-- {
			key := fmt.Sprintf("key%04d", i)
			err := bt.Delete(bp, []byte(key))
			assert.NoError(t, err)
		}

		// THEN: B+Tree が空になる
		records := bt.collectAllRecords(bp)
		assert.Equal(t, 0, len(records))
	})

	t.Run("不規則な順序で全レコードを削除しても B+Tree が壊れない", func(t *testing.T) {
		// GIVEN: 多数のレコードを挿入
		bt, bp := setupBTree(t)
		numRecords := 200
		keys := make([]string, numRecords)
		for i := range numRecords {
			keys[i] = fmt.Sprintf("key%04d", i)
			val := fmt.Sprintf("val%04d", i)
			bt.mustInsert(bp, keys[i], val)
		}

		// WHEN: 中間→先頭→末尾の順で削除 (決定論的な不規則順序)
		deleteOrder := make([]string, 0, numRecords)
		for i := numRecords / 2; i < numRecords; i++ {
			deleteOrder = append(deleteOrder, keys[i])
		}
		for i := 0; i < numRecords/2; i++ {
			deleteOrder = append(deleteOrder, keys[i])
		}
		for _, key := range deleteOrder {
			err := bt.Delete(bp, []byte(key))
			assert.NoError(t, err)
		}

		// THEN: B+Tree が空になる
		records := bt.collectAllRecords(bp)
		assert.Equal(t, 0, len(records))
	})

	for _, n := range []int{200, 300, 500} {
		t.Run(fmt.Sprintf("%d件の順次削除でデータが失われない", n), func(t *testing.T) {
			// GIVEN
			bt, bp := setupBTree(t)
			for i := range n {
				key := fmt.Sprintf("key%04d", i)
				val := fmt.Sprintf("val%04d", i)
				bt.mustInsert(bp, key, val)
			}

			// WHEN: 全件を順次削除
			for i := range n {
				key := fmt.Sprintf("key%04d", i)
				err := bt.Delete(bp, []byte(key))
				if err != nil {
					t.Fatalf("i=%d key=%s でエラー: %v", i, key, err)
				}
			}

			// THEN
			records := bt.collectAllRecords(bp)
			assert.Equal(t, 0, len(records))
		})
	}

	t.Run("削除ごとにレコード数が正しい", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		numRecords := 200
		for i := range numRecords {
			key := fmt.Sprintf("key%04d", i)
			val := fmt.Sprintf("val%04d", i)
			bt.mustInsert(bp, key, val)
		}

		// WHEN/THEN: 1 件削除するたびに残りのレコード数が正しい
		for i := range numRecords {
			key := fmt.Sprintf("key%04d", i)
			err := bt.Delete(bp, []byte(key))
			if err != nil {
				t.Fatalf("i=%d key=%s でエラー: %v", i, key, err)
			}
			remaining := bt.collectAllRecords(bp)
			expected := numRecords - i - 1
			if len(remaining) != expected {
				t.Fatalf("i=%d 削除後: レコード数=%d (期待=%d)", i, len(remaining), expected)
			}
		}
	})

	t.Run("空のツリーから削除するとエラーが返る", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)

		// WHEN
		err := bt.Delete(bp, []byte("nonexistent"))

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("削除後に新たに挿入できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "key1", "val1")
		bt.mustInsert(bp, "key2", "val2")
		err := bt.Delete(bp, []byte("key1"))
		assert.NoError(t, err)

		// WHEN
		err = bt.Insert(bp, node.NewRecord(nil, []byte("key3"), []byte("val3")))

		// THEN
		assert.NoError(t, err)
		records := bt.collectAllRecords(bp)
		assert.Equal(t, 2, len(records))
		assert.Equal(t, "key2", string(records[0].KeyBytes()))
		assert.Equal(t, "key3", string(records[1].KeyBytes()))
	})
}

func TestUpdate(t *testing.T) {
	t.Run("value を更新できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "key1", "val1")
		bt.mustInsert(bp, "key2", "val2")
		bt.mustInsert(bp, "key3", "val3")

		// WHEN
		err := bt.Update(bp, node.NewRecord(nil, []byte("key2"), []byte("updated")))

		// THEN
		assert.NoError(t, err)
		records := bt.collectAllRecords(bp)
		assert.Equal(t, 3, len(records))
		assert.Equal(t, "key1", string(records[0].KeyBytes()))
		assert.Equal(t, "val1", string(records[0].NonKeyBytes()))
		assert.Equal(t, "key2", string(records[1].KeyBytes()))
		assert.Equal(t, "updated", string(records[1].NonKeyBytes()))
		assert.Equal(t, "key3", string(records[2].KeyBytes()))
		assert.Equal(t, "val3", string(records[2].NonKeyBytes()))
	})

	t.Run("存在しないキーを更新するとエラーが返る", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "key1", "val1")

		// WHEN
		err := bt.Update(bp, node.NewRecord(nil, []byte("nonexistent"), []byte("val")))

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("空のツリーで更新するとエラーが返る", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)

		// WHEN
		err := bt.Update(bp, node.NewRecord(nil, []byte("key1"), []byte("val1")))

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("分割が発生した B+Tree で value を更新できる", func(t *testing.T) {
		// GIVEN: 多数のレコードを挿入してノード分割を発生させる
		bt, bp := setupBTree(t)
		numRecords := 100
		for i := range numRecords {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bp, key, val)
		}

		// WHEN: 複数のレコードを更新
		err := bt.Update(bp, node.NewRecord(nil, []byte("key000"), []byte("new000")))
		assert.NoError(t, err)
		err = bt.Update(bp, node.NewRecord(nil, []byte("key050"), []byte("new050")))
		assert.NoError(t, err)
		err = bt.Update(bp, node.NewRecord(nil, []byte("key099"), []byte("new099")))
		assert.NoError(t, err)

		// THEN: 更新されたレコードが正しく取得でき、他のレコードは変わらない
		records := bt.collectAllRecords(bp)
		assert.Equal(t, numRecords, len(records))
		assert.Equal(t, "new000", string(records[0].NonKeyBytes()))
		assert.Equal(t, "val001", string(records[1].NonKeyBytes()))
		assert.Equal(t, "new050", string(records[50].NonKeyBytes()))
		assert.Equal(t, "new099", string(records[99].NonKeyBytes()))
	})

	t.Run("更新後に Search で正しい値が取得できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")

		// WHEN
		err := bt.Update(bp, node.NewRecord(nil, []byte("bbb"), []byte("updated_v2")))
		assert.NoError(t, err)

		// THEN: SearchModeKey で更新後の値が取得できる
		iter, err := bt.Search(bp, SearchModeKey{Key: []byte("bbb")})
		assert.NoError(t, err)
		record, ok := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, "bbb", string(record.KeyBytes()))
		assert.Equal(t, "updated_v2", string(record.NonKeyBytes()))
	})

	t.Run("value のサイズが大きく変わる更新ができる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "key1", "v1")
		bt.mustInsert(bp, "key2", "v2")
		bt.mustInsert(bp, "key3", "v3")

		// WHEN: 短い value を長い value に更新
		longValue := make([]byte, 500)
		for i := range longValue {
			longValue[i] = 'x'
		}
		err := bt.Update(bp, node.NewRecord(nil, []byte("key2"), longValue))

		// THEN
		assert.NoError(t, err)
		records := bt.collectAllRecords(bp)
		assert.Equal(t, 3, len(records))
		assert.Equal(t, "v1", string(records[0].NonKeyBytes()))
		assert.Equal(t, longValue, records[1].NonKeyBytes())
		assert.Equal(t, "v3", string(records[2].NonKeyBytes()))
	})

	t.Run("ページに収まらない大きな value への更新はエラーが返る", func(t *testing.T) {
		// GIVEN: ノードをほぼ満杯にする
		bt, bp := setupBTree(t)
		value := make([]byte, 200)
		numRecords := 15
		for i := range numRecords {
			key := fmt.Sprintf("key%03d", i)
			bt.mustInsert(bp, key, string(value))
		}

		// WHEN: 非常に大きな value に更新を試みる
		hugeValue := make([]byte, 3000)
		err := bt.Update(bp, node.NewRecord(nil, []byte("key000"), hugeValue))

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to update record")
	})

	t.Run("同じキーを複数回更新できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "key1", "val1")

		// WHEN: 3 回連続で更新
		err := bt.Update(bp, node.NewRecord(nil, []byte("key1"), []byte("second")))
		assert.NoError(t, err)
		err = bt.Update(bp, node.NewRecord(nil, []byte("key1"), []byte("third")))
		assert.NoError(t, err)
		err = bt.Update(bp, node.NewRecord(nil, []byte("key1"), []byte("final")))
		assert.NoError(t, err)

		// THEN: 最後の値が反映されている
		records := bt.collectAllRecords(bp)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, "final", string(records[0].NonKeyBytes()))
	})
}

func TestFindByKey(t *testing.T) {
	t.Run("存在するキーのレコードを取得できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")

		// WHEN
		record, err := bt.FindByKey(bp, []byte("bbb"))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "bbb", string(record.KeyBytes()))
		assert.Equal(t, "v2", string(record.NonKeyBytes()))
	})

	t.Run("先頭のキーを取得できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")

		// WHEN
		record, err := bt.FindByKey(bp, []byte("aaa"))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "aaa", string(record.KeyBytes()))
		assert.Equal(t, "v1", string(record.NonKeyBytes()))
	})

	t.Run("末尾のキーを取得できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")

		// WHEN
		record, err := bt.FindByKey(bp, []byte("bbb"))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "bbb", string(record.KeyBytes()))
		assert.Equal(t, "v2", string(record.NonKeyBytes()))
	})

	t.Run("存在しないキーで検索すると ErrKeyNotFound が返る", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "ccc", "v3")

		// WHEN
		_, err := bt.FindByKey(bp, []byte("bbb"))

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("空のツリーで検索すると ErrKeyNotFound が返る", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)

		// WHEN
		_, err := bt.FindByKey(bp, []byte("aaa"))

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("最大キーより大きいキーで検索すると ErrKeyNotFound が返る", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")

		// WHEN
		_, err := bt.FindByKey(bp, []byte("zzz"))

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("分割が発生した B+Tree でもキーを取得できる", func(t *testing.T) {
		// GIVEN: 多数のレコードを挿入してノード分割を発生させる
		bt, bp := setupBTree(t)
		numRecords := 100
		for i := range numRecords {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bp, key, val)
		}

		// WHEN: 先頭・中間・末尾のキーを検索
		first, err := bt.FindByKey(bp, []byte("key000"))
		assert.NoError(t, err)
		mid, err := bt.FindByKey(bp, []byte("key050"))
		assert.NoError(t, err)
		last, err := bt.FindByKey(bp, []byte("key099"))
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, "val000", string(first.NonKeyBytes()))
		assert.Equal(t, "val050", string(mid.NonKeyBytes()))
		assert.Equal(t, "val099", string(last.NonKeyBytes()))
	})

	t.Run("Update 後に FindByKey で更新された値が取得できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "key1", "val1")

		err := bt.Update(bp, node.NewRecord(nil, []byte("key1"), []byte("updated")))
		assert.NoError(t, err)

		// WHEN
		record, err := bt.FindByKey(bp, []byte("key1"))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "updated", string(record.NonKeyBytes()))
	})

	t.Run("Delete 後に FindByKey で検索すると ErrKeyNotFound が返る", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "key1", "val1")
		bt.mustInsert(bp, "key2", "val2")

		err := bt.Delete(bp, []byte("key1"))
		assert.NoError(t, err)

		// WHEN
		_, err = bt.FindByKey(bp, []byte("key1"))

		// THEN: 物理削除されているため見つからない
		assert.ErrorIs(t, err, ErrKeyNotFound)

		// 他のキーは取得できる
		record, err := bt.FindByKey(bp, []byte("key2"))
		assert.NoError(t, err)
		assert.Equal(t, "val2", string(record.NonKeyBytes()))
	})
}

func TestNewBTree(t *testing.T) {
	t.Run("既存の B+Tree を NewBTree で開いてデータを読み取れる", func(t *testing.T) {
		// GIVEN: CreateBTree でツリーを作成しレコードを挿入する
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")

		// WHEN: 同じ metaPageId で NewBTree を呼ぶ
		bt2 := NewBTree(bt.MetaPageId)

		// THEN: 挿入したレコードがすべて取得できる
		records := bt2.collectAllRecords(bp)
		assert.Equal(t, 3, len(records))
		assert.Equal(t, "aaa", string(records[0].KeyBytes()))
		assert.Equal(t, "bbb", string(records[1].KeyBytes()))
		assert.Equal(t, "ccc", string(records[2].KeyBytes()))
	})
}

func TestLeafPageCount(t *testing.T) {
	t.Run("作成直後のリーフページ数は 1", func(t *testing.T) {
		// GIVEN & WHEN
		bt, bp := setupBTree(t)

		// THEN
		count, err := bt.LeafPageCount(bp)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})

	t.Run("リーフ分割が発生するとリーフページ数が増加する", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)

		// WHEN: 多数のレコードを挿入してリーフ分割を発生させる
		numRecords := 500
		for i := range numRecords {
			key := fmt.Sprintf("key%04d", i)
			val := fmt.Sprintf("val%04d", i)
			bt.mustInsert(bp, key, val)
		}

		// THEN: リーフページ数が 1 より大きい
		count, err := bt.LeafPageCount(bp)
		assert.NoError(t, err)
		assert.Greater(t, count, uint64(1))
	})

	t.Run("削除するとリーフページ数が減少する", func(t *testing.T) {
		// GIVEN: 多数のレコードを挿入してリーフ分割を発生させる
		bt, bp := setupBTree(t)
		numRecords := 500
		for i := range numRecords {
			key := fmt.Sprintf("key%04d", i)
			val := fmt.Sprintf("val%04d", i)
			bt.mustInsert(bp, key, val)
		}
		countBefore, err := bt.LeafPageCount(bp)
		assert.NoError(t, err)
		assert.Greater(t, countBefore, uint64(1))

		// WHEN: 半分のレコードを削除
		for i := 0; i < numRecords; i += 2 {
			key := fmt.Sprintf("key%04d", i)
			err := bt.Delete(bp, []byte(key))
			assert.NoError(t, err)
		}

		// THEN: リーフページ数が減少している
		countAfter, err := bt.LeafPageCount(bp)
		assert.NoError(t, err)
		assert.Less(t, countAfter, countBefore)
	})

	t.Run("Insert と Delete を繰り返してもリーフページ数が 1 以上", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)

		// WHEN: 500 件挿入 → 250 件削除
		numRecords := 500
		for i := range numRecords {
			key := fmt.Sprintf("key%04d", i)
			val := fmt.Sprintf("val%04d", i)
			bt.mustInsert(bp, key, val)
		}
		for i := 0; i < numRecords; i += 2 {
			key := fmt.Sprintf("key%04d", i)
			err := bt.Delete(bp, []byte(key))
			assert.NoError(t, err)
		}

		// THEN: リーフページ数が 1 以上で、全レコードが正しく取得できる
		count, err := bt.LeafPageCount(bp)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, count, uint64(1))
		records := bt.collectAllRecords(bp)
		assert.Equal(t, numRecords/2, len(records))
	})
}

func TestHeight(t *testing.T) {
	t.Run("作成直後の高さは 1", func(t *testing.T) {
		// GIVEN & WHEN
		bt, bp := setupBTree(t)

		// THEN
		h, err := bt.Height(bp)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), h)
	})

	t.Run("ルート分割が発生すると高さが増加する", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)

		// WHEN: 多数のレコードを挿入してルート分割を発生させる
		numRecords := 500
		for i := range numRecords {
			key := fmt.Sprintf("key%04d", i)
			val := fmt.Sprintf("val%04d", i)
			bt.mustInsert(bp, key, val)
		}

		// THEN: 高さが 1 より大きい
		h, err := bt.Height(bp)
		assert.NoError(t, err)
		assert.Greater(t, h, uint64(1))
	})

	t.Run("削除しても高さは増加しない", func(t *testing.T) {
		// GIVEN: 多数のレコードを挿入してルート分割を発生させる
		bt, bp := setupBTree(t)
		numRecords := 500
		for i := range numRecords {
			key := fmt.Sprintf("key%04d", i)
			val := fmt.Sprintf("val%04d", i)
			bt.mustInsert(bp, key, val)
		}
		hBefore, err := bt.Height(bp)
		assert.NoError(t, err)
		assert.Greater(t, hBefore, uint64(1))

		// WHEN: 半分のレコードを削除
		for i := 0; i < numRecords; i += 2 {
			key := fmt.Sprintf("key%04d", i)
			err := bt.Delete(bp, []byte(key))
			assert.NoError(t, err)
		}

		// THEN: 高さが増加していない
		hAfter, err := bt.Height(bp)
		assert.NoError(t, err)
		assert.LessOrEqual(t, hAfter, hBefore)
	})
}

// テスト用の B+Tree とバッファプールマネージャをセットアップする
func setupBTree(t *testing.T) (*BTree, *buffer.BufferPool) {
	t.Helper()
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "btree_test.db")
	fileId := page.FileId(0)
	dm, err := file.NewDisk(fileId, path)
	if err != nil {
		t.Fatalf("Disk の作成に失敗: %v", err)
	}
	metaPageId := dm.AllocatePage()

	bp := buffer.NewBufferPool(100)
	bp.RegisterDisk(fileId, dm)

	bt, err := CreateBTree(bp, metaPageId)
	if err != nil {
		t.Fatalf("B+Tree の作成に失敗: %v", err)
	}
	return bt, bp
}

// レコードを挿入するヘルパー (エラー時は panic)
func (bt *BTree) mustInsert(bp *buffer.BufferPool, key, value string) {
	err := bt.Insert(bp, node.NewRecord(nil, []byte(key), []byte(value)))
	if err != nil {
		panic(fmt.Sprintf("Insert に失敗: %v", err))
	}
}

// B+Tree の全レコードをイテレータで収集する
func (bt *BTree) collectAllRecords(bp *buffer.BufferPool) []node.Record {
	iter, err := bt.Search(bp, SearchModeStart{})
	if err != nil {
		panic(fmt.Sprintf("Search に失敗: %v", err))
	}
	var records []node.Record
	for {
		record, ok, err := iter.Next(bp)
		if err != nil {
			panic(fmt.Sprintf("Next に失敗: %v", err))
		}
		if !ok {
			break
		}
		// レコードのヘッダ、キー、非キーをコピー (underlying data への参照を避ける)
		headerCopy := make([]byte, len(record.HeaderBytes()))
		copy(headerCopy, record.HeaderBytes())
		keyCopy := make([]byte, len(record.KeyBytes()))
		copy(keyCopy, record.KeyBytes())
		nonKeyCopy := make([]byte, len(record.NonKeyBytes()))
		copy(nonKeyCopy, record.NonKeyBytes())
		records = append(records, node.NewRecord(headerCopy, keyCopy, nonKeyCopy))
	}
	return records
}
