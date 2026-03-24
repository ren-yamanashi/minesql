package access

import (
	"fmt"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/memcomparable"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Create と Insert をテスト
func TestUniqueIndex(t *testing.T) {
	t.Run("ユニークインデックスの作成ができ、そのインデックスに値が挿入できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "test.db")

		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetapageId, 0)

		// WHEN: ユニークインデックスを作成
		err = uniqueIndex.Create(bp)
		assert.NoError(t, err)

		// WHEN: インデックスに値を挿入
		err = uniqueIndex.Insert(bp, []uint8{0}, [][]byte{[]byte("John")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, []uint8{1}, [][]byte{[]byte("Alice")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, []uint8{2}, [][]byte{[]byte("Eve")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, []uint8{3}, [][]byte{[]byte("Bob")})
		assert.NoError(t, err)

		// THEN: 挿入したデータが B+Tree に存在する
		tree := btree.NewBPlusTree(uniqueIndex.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		expectedRecords := []struct {
			key   [][]byte
			value []uint8
		}{
			// キーの順序でソートされる (プライマリキーの順序ではない)
			{[][]byte{[]byte("Alice")}, []uint8{1}},
			{[][]byte{[]byte("Bob")}, []uint8{3}},
			{[][]byte{[]byte("Eve")}, []uint8{2}},
			{[][]byte{[]byte("John")}, []uint8{0}},
		}

		i := 0
		for {
			pair, ok := iter.Get()
			if !ok {
				break
			}
			expected := expectedRecords[i]

			// エンコードされたキーをデコード
			var decodedKey [][]byte
			keyBytes := pair.Key
			memcomparable.Decode(keyBytes, &decodedKey)

			assert.Equal(t, expected.key, decodedKey)
			assert.Equal(t, expected.value, pair.Value)

			i++
			_, _, err := iter.Next(bp)
			assert.NoError(t, err)
		}
	})
}

func TestUniqueIndexDelete(t *testing.T) {
	t.Run("ユニークインデックスから行を削除できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "test.db")

		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetapageId, 0)

		err = uniqueIndex.Create(bp)
		assert.NoError(t, err)

		err = uniqueIndex.Insert(bp, []uint8{0}, [][]byte{[]byte("John")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, []uint8{1}, [][]byte{[]byte("Alice")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, []uint8{2}, [][]byte{[]byte("Eve")})
		assert.NoError(t, err)

		// WHEN: "Alice" を削除
		err = uniqueIndex.Delete(bp, [][]byte{[]byte("Alice")})

		// THEN: "Alice" が削除され、残りが昇順で取得できる
		assert.NoError(t, err)
		tree := btree.NewBPlusTree(uniqueIndex.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		expectedRecords := []struct {
			key   [][]byte
			value []uint8
		}{
			{[][]byte{[]byte("Eve")}, []uint8{2}},
			{[][]byte{[]byte("John")}, []uint8{0}},
		}

		i := 0
		for {
			pair, ok := iter.Get()
			if !ok {
				break
			}
			expected := expectedRecords[i]

			var decodedKey [][]byte
			memcomparable.Decode(pair.Key, &decodedKey)

			assert.Equal(t, expected.key, decodedKey)
			assert.Equal(t, expected.value, pair.Value)

			i++
			_, _, err := iter.Next(bp)
			assert.NoError(t, err)
		}
		assert.Equal(t, len(expectedRecords), i)
	})
}


func TestUniqueIndexLeafPageCount(t *testing.T) {
	t.Run("作成直後のテーブルのリーフページ数は1", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "test.db")
		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetaPageId, 0)

		err = uniqueIndex.Create(bp)
		assert.NoError(t, err)

		// WHEN
		leafPageCount, err := uniqueIndex.LeafPageCount(bp)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), leafPageCount)
	})

	t.Run("データ挿入によりリーフページが分割されるとリーフページ数が増加する", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "test.db")
		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetaPageId, 0)

		err = uniqueIndex.Create(bp)
		assert.NoError(t, err)

		// WHEN: 十分な量のデータを挿入してリーフページ分割を発生させる
		for i := range 500 {
			key := fmt.Sprintf("key_%04d", i)
			primaryKey := []byte(fmt.Sprintf("pk_%04d", i))
			err := uniqueIndex.Insert(bp, primaryKey, [][]byte{[]byte(key)})
			assert.NoError(t, err)
		}

		// THEN
		leafPageCount, err := uniqueIndex.LeafPageCount(bp)
		assert.NoError(t, err)
		assert.Greater(t, leafPageCount, uint64(1))
	})
}

func TestUniqueIndexHeight(t *testing.T) {
	t.Run("作成直後のインデックスの高さは1", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "test.db")
		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetaPageId, 0)
		err = uniqueIndex.Create(bp)
		assert.NoError(t, err)

		// WHEN
		height, err := uniqueIndex.Height(bp)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})

	t.Run("データ挿入によりルート分割が発生すると高さが増加する", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "test.db")
		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetaPageId, 0)
		err = uniqueIndex.Create(bp)
		assert.NoError(t, err)

		// WHEN: 十分な量のデータを挿入してルート分割を発生させる
		for i := range 500 {
			key := fmt.Sprintf("key_%04d", i)
			primaryKey := []byte(fmt.Sprintf("pk_%04d", i))
			err := uniqueIndex.Insert(bp, primaryKey, [][]byte{[]byte(key)})
			assert.NoError(t, err)
		}

		// THEN
		height, err := uniqueIndex.Height(bp)
		assert.NoError(t, err)
		assert.Greater(t, height, uint64(1))
	})
}
