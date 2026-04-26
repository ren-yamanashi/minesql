package btree

import (
	"fmt"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/stretchr/testify/assert"
)

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

	t.Run("削除したキーを再挿入できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "key1", "val1")
		bt.mustInsert(bp, "key2", "val2")
		err := bt.Delete(bp, []byte("key1"))
		assert.NoError(t, err)

		// WHEN: 削除したキーを再挿入
		err = bt.Insert(bp, node.NewRecord(nil, []byte("key1"), []byte("new_val1")))

		// THEN
		assert.NoError(t, err)
		record, _, err := bt.FindByKey(bp, []byte("key1"))
		assert.NoError(t, err)
		assert.Equal(t, "new_val1", string(record.NonKeyBytes()))
	})
}
