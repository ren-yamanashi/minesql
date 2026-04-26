package btree

import (
	"fmt"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/stretchr/testify/assert"
)

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
