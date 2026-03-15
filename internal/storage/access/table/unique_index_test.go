package table

import (
	"minesql/internal/storage/access/btree"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Create と Insert をテスト
func TestUniqueIndex(t *testing.T) {
	t.Run("ユニークインデックスの作成ができ、そのインデックスに値が挿入できる", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewUniqueIndex("test_index", "test", 0)
		bp, metaPageId, _ := InitDisk(t, "test.db")

		// UniqueIndex の metaPageId を割り当て
		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex.MetaPageId = indexMetapageId

		// WHEN: ユニークインデックスを作成
		err = uniqueIndex.Create(bp, indexMetapageId)
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
		tree := btree.NewBTree(uniqueIndex.MetaPageId)
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
			Decode(keyBytes, &decodedKey)

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
		uniqueIndex := NewUniqueIndex("test_index", "test", 0)
		bp, metaPageId, _ := InitDisk(t, "test.db")

		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex.MetaPageId = indexMetapageId

		err = uniqueIndex.Create(bp, indexMetapageId)
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
		tree := btree.NewBTree(uniqueIndex.MetaPageId)
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
			Decode(pair.Key, &decodedKey)

			assert.Equal(t, expected.key, decodedKey)
			assert.Equal(t, expected.value, pair.Value)

			i++
			_, _, err := iter.Next(bp)
			assert.NoError(t, err)
		}
		assert.Equal(t, len(expectedRecords), i)
	})
}

func TestUniqueIndexUpdate(t *testing.T) {
	t.Run("セカンダリキーが変わる場合、Delete + Insert が行われる", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewUniqueIndex("test_index", "test", 0)
		bp, metaPageId, _ := InitDisk(t, "test.db")

		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex.MetaPageId = indexMetapageId

		err = uniqueIndex.Create(bp, indexMetapageId)
		assert.NoError(t, err)

		err = uniqueIndex.Insert(bp, []uint8{0}, [][]byte{[]byte("John")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, []uint8{1}, [][]byte{[]byte("Alice")})
		assert.NoError(t, err)

		// WHEN: "John" → "Zack" に変更
		oldRecord := [][]byte{[]byte("John")}
		newRecord := [][]byte{[]byte("Zack")}
		err = uniqueIndex.Update(bp, oldRecord, newRecord, []uint8{0})

		// THEN: "John" が削除され "Zack" が追加されている
		assert.NoError(t, err)
		tree := btree.NewBTree(uniqueIndex.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		var keys []string
		for {
			pair, ok, err := iter.Next(bp)
			assert.NoError(t, err)
			if !ok {
				break
			}
			var decodedKey [][]byte
			Decode(pair.Key, &decodedKey)
			keys = append(keys, string(decodedKey[0]))
		}
		assert.Equal(t, []string{"Alice", "Zack"}, keys)
	})

	t.Run("セカンダリキーが同じでプライマリキーが変わる場合、value が更新される", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewUniqueIndex("test_index", "test", 0)
		bp, metaPageId, _ := InitDisk(t, "test.db")

		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex.MetaPageId = indexMetapageId

		err = uniqueIndex.Create(bp, indexMetapageId)
		assert.NoError(t, err)

		err = uniqueIndex.Insert(bp, []uint8{0}, [][]byte{[]byte("John")})
		assert.NoError(t, err)

		// WHEN: セカンダリキーは "John" のまま、プライマリキーを {0} → {99} に変更
		oldRecord := [][]byte{[]byte("John")}
		newRecord := [][]byte{[]byte("John")}
		err = uniqueIndex.Update(bp, oldRecord, newRecord, []uint8{99})

		// THEN: キーは同じで value (プライマリキー) が更新されている
		assert.NoError(t, err)
		tree := btree.NewBTree(uniqueIndex.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		pair, ok, err := iter.Next(bp)
		assert.NoError(t, err)
		assert.True(t, ok)

		var decodedKey [][]byte
		Decode(pair.Key, &decodedKey)
		assert.Equal(t, "John", string(decodedKey[0]))
		assert.Equal(t, []uint8{99}, pair.Value)
	})

	t.Run("セカンダリキーを既存の値に変更するとエラーが返る", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewUniqueIndex("test_index", "test", 0)
		bp, metaPageId, _ := InitDisk(t, "test.db")

		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex.MetaPageId = indexMetapageId

		err = uniqueIndex.Create(bp, indexMetapageId)
		assert.NoError(t, err)

		err = uniqueIndex.Insert(bp, []uint8{0}, [][]byte{[]byte("Alice")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, []uint8{1}, [][]byte{[]byte("John")})
		assert.NoError(t, err)

		// WHEN: "John" → "Alice" に変更 (既存の値と衝突)
		oldRecord := [][]byte{[]byte("John")}
		newRecord := [][]byte{[]byte("Alice")}
		err = uniqueIndex.Update(bp, oldRecord, newRecord, []uint8{1})

		// THEN: Delete は成功するが Insert が重複キーエラーで失敗する
		assert.Error(t, err)
	})

	t.Run("存在しない旧セカンダリキーで更新するとエラーが返る", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewUniqueIndex("test_index", "test", 0)
		bp, metaPageId, _ := InitDisk(t, "test.db")

		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex.MetaPageId = indexMetapageId

		err = uniqueIndex.Create(bp, indexMetapageId)
		assert.NoError(t, err)

		err = uniqueIndex.Insert(bp, []uint8{0}, [][]byte{[]byte("Alice")})
		assert.NoError(t, err)

		// WHEN: 存在しない旧キーで更新
		oldRecord := [][]byte{[]byte("NonExistent")}
		newRecord := [][]byte{[]byte("Bob")}
		err = uniqueIndex.Update(bp, oldRecord, newRecord, []uint8{0})

		// THEN
		assert.Error(t, err)
	})

	t.Run("セカンダリキーもプライマリキーも同じ場合、データが変わらない", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewUniqueIndex("test_index", "test", 0)
		bp, metaPageId, _ := InitDisk(t, "test.db")

		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex.MetaPageId = indexMetapageId

		err = uniqueIndex.Create(bp, indexMetapageId)
		assert.NoError(t, err)

		err = uniqueIndex.Insert(bp, []uint8{0}, [][]byte{[]byte("John")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, []uint8{1}, [][]byte{[]byte("Alice")})
		assert.NoError(t, err)

		// WHEN: キーも値も変わらない更新
		oldRecord := [][]byte{[]byte("John")}
		newRecord := [][]byte{[]byte("John")}
		err = uniqueIndex.Update(bp, oldRecord, newRecord, []uint8{0})

		// THEN: データが変わらない
		assert.NoError(t, err)
		tree := btree.NewBTree(uniqueIndex.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		expectedRecords := []struct {
			key   string
			value []uint8
		}{
			{"Alice", []uint8{1}},
			{"John", []uint8{0}},
		}

		i := 0
		for {
			pair, ok, err := iter.Next(bp)
			assert.NoError(t, err)
			if !ok {
				break
			}
			var decodedKey [][]byte
			Decode(pair.Key, &decodedKey)
			assert.Equal(t, expectedRecords[i].key, string(decodedKey[0]))
			assert.Equal(t, expectedRecords[i].value, pair.Value)
			i++
		}
		assert.Equal(t, len(expectedRecords), i)
	})
}
