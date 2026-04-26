package btree

import (
	"fmt"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/stretchr/testify/assert"
)

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
