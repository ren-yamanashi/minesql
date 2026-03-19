package table

import (
	"minesql/internal/storage/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
	"minesql/internal/storage/page"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateAndInsert(t *testing.T) {
	t.Run("テーブルの作成ができ、そのテーブルに値が挿入できる", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewUniqueIndex("idx_last_name", "last_name", 2)
		bp, metaPageId, _ := InitDisk(t, "users.db")

		// UniqueIndex の metaPageId を割り当て
		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex.MetaPageId = indexMetaPageId

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex})

		// WHEN: テーブルを作成
		err = table.Create(bp)
		assert.NoError(t, err)

		// WHEN: 行を挿入
		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("d"), []byte("Eve"), []byte("Davis")})
		assert.NoError(t, err)

		// THEN: 挿入したデータが B+Tree に存在する
		tree := btree.NewBPlusTree(table.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		expectedRecords := []struct {
			key   [][]byte
			value [][]byte
		}{
			{[][]byte{[]byte("a")}, [][]byte{[]byte("John"), []byte("Doe")}},
			{[][]byte{[]byte("b")}, [][]byte{[]byte("Alice"), []byte("Smith")}},
			{[][]byte{[]byte("c")}, [][]byte{[]byte("Bob"), []byte("Johnson")}},
			{[][]byte{[]byte("d")}, [][]byte{[]byte("Eve"), []byte("Davis")}},
		}

		i := 0
		for {
			pair, ok := iter.Get()
			if !ok {
				break
			}
			expected := expectedRecords[i]

			// エンコードされたキーと値をデコード
			var decodedKey [][]byte
			var decodedValue [][]byte
			keyBytes := pair.Key
			valueBytes := pair.Value
			Decode(keyBytes, &decodedKey)
			Decode(valueBytes, &decodedValue)

			assert.Equal(t, expected.key, decodedKey)
			assert.Equal(t, expected.value, decodedValue)

			i++
			_, _, err := iter.Next(bp)
			assert.NoError(t, err)
		}
		assert.Equal(t, len(expectedRecords), i)

		// THEN: ユニークインデックスにもデータが挿入されている
		uniqueIndexTree := btree.NewBPlusTree(table.UniqueIndexes[0].MetaPageId)
		uniqueIndexIter, err := uniqueIndexTree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		// SecondaryKey = 2 なので、3 番目のカラム (姓) がキー、エンコードされたプライマリキーが値
		// プライマリキーをエンコード
		var encodedPrimaryKeyA, encodedPrimaryKeyB, encodedPrimaryKeyC, encodedPrimaryKeyD []byte
		Encode([][]byte{[]byte("a")}, &encodedPrimaryKeyA)
		Encode([][]byte{[]byte("b")}, &encodedPrimaryKeyB)
		Encode([][]byte{[]byte("c")}, &encodedPrimaryKeyC)
		Encode([][]byte{[]byte("d")}, &encodedPrimaryKeyD)

		expectedUniqueIndexRecords := []struct {
			key   [][]byte
			value []uint8
		}{
			// キーの順序でソートされる
			{[][]byte{[]byte("Davis")}, encodedPrimaryKeyD},
			{[][]byte{[]byte("Doe")}, encodedPrimaryKeyA},
			{[][]byte{[]byte("Johnson")}, encodedPrimaryKeyC},
			{[][]byte{[]byte("Smith")}, encodedPrimaryKeyB},
		}

		j := 0
		for {
			pair, ok := uniqueIndexIter.Get()
			if !ok {
				break
			}
			expected := expectedUniqueIndexRecords[j]

			// エンコードされたキーをデコード
			var decodedKey [][]byte
			Decode(pair.Key, &decodedKey)

			assert.Equal(t, expected.key, decodedKey)
			assert.Equal(t, expected.value, pair.Value)

			j++
			_, _, err := uniqueIndexIter.Next(bp)
			assert.NoError(t, err)
		}
		assert.Equal(t, len(expectedUniqueIndexRecords), j)
	})

	t.Run("テーブルとそのインデックスが同じディスクファイル (同じ FileId) に保存される", func(t *testing.T) {
		// GIVEN
		// 2つのインデックスを持つテーブルを作成
		uniqueIndex1 := NewUniqueIndex("idx_first_name", "first_name", 1)
		uniqueIndex2 := NewUniqueIndex("idx_last_name", "last_name", 2)
		bp, metaPageId, tmpdir := InitDisk(t, "users.db")

		// UniqueIndex の metaPageId を割り当て
		indexMetaPageId1, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex1.MetaPageId = indexMetaPageId1
		indexMetaPageId2, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex2.MetaPageId = indexMetaPageId2

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex1, uniqueIndex2})

		// WHEN
		err = table.Create(bp)
		assert.NoError(t, err)

		// THEN: テーブルとすべてのインデックスが同じ FileId を持つ
		assert.Equal(t, table.MetaPageId.FileId, uniqueIndex1.MetaPageId.FileId, "first_name インデックスはテーブルと同じ FileId を持つべき")
		assert.Equal(t, table.MetaPageId.FileId, uniqueIndex2.MetaPageId.FileId, "last_name インデックスはテーブルと同じ FileId を持つべき")

		// THEN: MetaPageId は異なる (各 B+Tree は別々のメタページを持つ)
		assert.NotEqual(t, table.MetaPageId, uniqueIndex1.MetaPageId, "テーブルとインデックスは異なる MetaPageId を持つべき")
		assert.NotEqual(t, table.MetaPageId, uniqueIndex2.MetaPageId, "テーブルとインデックスは異なる MetaPageId を持つべき")
		assert.NotEqual(t, uniqueIndex1.MetaPageId, uniqueIndex2.MetaPageId, "各インデックスは異なる MetaPageId を持つべき")

		// THEN: ディスクに作成されたファイルが1つだけである
		files, err := os.ReadDir(tmpdir)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(files), "ディスクファイルは1つだけ作成されるべき")
		assert.Equal(t, "users.db", files[0].Name(), "ファイル名はテーブル名.db であるべき")

		// THEN: ファイルパスが正しい
		expectedFilePath := filepath.Join(tmpdir, "users.db")
		_, err = os.Stat(expectedFilePath)
		assert.NoError(t, err, "users.db ファイルが存在するべき")
	})
}

func TestDelete(t *testing.T) {
	t.Run("テーブルから行を削除でき、B+Tree とユニークインデックスの両方から削除される", func(t *testing.T) {
		// GIVEN: テーブルを作成しデータを挿入
		uniqueIndex := NewUniqueIndex("idx_last_name", "last_name", 2)
		bp, metaPageId, _ := InitDisk(t, "users.db")

		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex.MetaPageId = indexMetaPageId

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex})
		err = table.Create(bp)
		assert.NoError(t, err)

		records := [][]byte{[]byte("a"), []byte("John"), []byte("Doe")}
		err = table.Insert(bp, records)
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
		assert.NoError(t, err)

		// WHEN: "a" の行を削除
		err = table.Delete(bp, records)

		// THEN: B+Tree から削除されている
		assert.NoError(t, err)
		tree := btree.NewBPlusTree(table.MetaPageId)
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
		assert.Equal(t, []string{"b", "c"}, keys)

		// THEN: ユニークインデックスからも削除されている
		indexTree := btree.NewBPlusTree(table.UniqueIndexes[0].MetaPageId)
		indexIter, err := indexTree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		var indexKeys []string
		for {
			pair, ok, err := indexIter.Next(bp)
			assert.NoError(t, err)
			if !ok {
				break
			}
			var decodedKey [][]byte
			Decode(pair.Key, &decodedKey)
			indexKeys = append(indexKeys, string(decodedKey[0]))
		}
		// "Doe" が削除されて "Johnson", "Smith" のみ残る
		assert.Equal(t, []string{"Johnson", "Smith"}, indexKeys)
	})

	t.Run("存在しないキーを削除するとエラーが返る", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)

		// WHEN: 存在しないキーで削除
		err = table.Delete(bp, [][]byte{[]byte("z"), []byte("Unknown")})

		// THEN
		assert.Error(t, err)
	})

	t.Run("全行を削除した後にテーブルが空になる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		record1 := [][]byte{[]byte("a"), []byte("John")}
		record2 := [][]byte{[]byte("b"), []byte("Alice")}
		err = table.Insert(bp, record1)
		assert.NoError(t, err)
		err = table.Insert(bp, record2)
		assert.NoError(t, err)

		// WHEN
		err = table.Delete(bp, record1)
		assert.NoError(t, err)
		err = table.Delete(bp, record2)
		assert.NoError(t, err)

		// THEN
		tree := btree.NewBPlusTree(table.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)
		_, ok := iter.Get()
		assert.False(t, ok)
	})
}

func TestUpdate(t *testing.T) {
	t.Run("プライマリキーが同じ場合、value のみが更新される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)

		// WHEN: プライマリキー "a" のレコードを更新 (キーは同じ、value のみ変更)
		oldRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Doe")}
		newRecord := [][]byte{[]byte("a"), []byte("Jane"), []byte("Doe-Updated")}
		err = table.Update(bp, oldRecord, newRecord)

		// THEN
		assert.NoError(t, err)
		tree := btree.NewBPlusTree(table.MetaPageId)
		pairs := collectAllTablePairs(t, bp, tree)
		assert.Equal(t, 2, len(pairs))
		// "a" の value が更新されている
		assert.Equal(t, [][]byte{[]byte("a")}, pairs[0].key)
		assert.Equal(t, [][]byte{[]byte("Jane"), []byte("Doe-Updated")}, pairs[0].value)
		// "b" は変わらない
		assert.Equal(t, [][]byte{[]byte("b")}, pairs[1].key)
		assert.Equal(t, [][]byte{[]byte("Alice"), []byte("Smith")}, pairs[1].value)
	})

	t.Run("プライマリキーが変わる場合、Delete + Insert が行われる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("c"), []byte("Bob")})
		assert.NoError(t, err)

		// WHEN: プライマリキーを "a" → "b" に変更
		oldRecord := [][]byte{[]byte("a"), []byte("John")}
		newRecord := [][]byte{[]byte("b"), []byte("John-Updated")}
		err = table.Update(bp, oldRecord, newRecord)

		// THEN
		assert.NoError(t, err)
		tree := btree.NewBPlusTree(table.MetaPageId)
		pairs := collectAllTablePairs(t, bp, tree)
		assert.Equal(t, 2, len(pairs))
		// "a" は削除され、"b" が挿入されている
		assert.Equal(t, [][]byte{[]byte("b")}, pairs[0].key)
		assert.Equal(t, [][]byte{[]byte("John-Updated")}, pairs[0].value)
		assert.Equal(t, [][]byte{[]byte("c")}, pairs[1].key)
		assert.Equal(t, [][]byte{[]byte("Bob")}, pairs[1].value)
	})

	t.Run("ユニークインデックスのセカンダリキーが変わる場合、インデックスも更新される", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewUniqueIndex("idx_last_name", "last_name", 2)
		bp, metaPageId, _ := InitDisk(t, "users.db")

		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex.MetaPageId = indexMetaPageId

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex})
		err = table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)

		// WHEN: セカンダリキー (last_name) を "Doe" → "Williams" に変更
		oldRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Doe")}
		newRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Williams")}
		err = table.Update(bp, oldRecord, newRecord)

		// THEN: ユニークインデックスが更新されている
		assert.NoError(t, err)
		indexTree := btree.NewBPlusTree(table.UniqueIndexes[0].MetaPageId)
		indexIter, err := indexTree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		var indexKeys []string
		for {
			pair, ok, err := indexIter.Next(bp)
			assert.NoError(t, err)
			if !ok {
				break
			}
			var decodedKey [][]byte
			Decode(pair.Key, &decodedKey)
			indexKeys = append(indexKeys, string(decodedKey[0]))
		}
		// "Doe" が削除され "Williams" が追加されている
		assert.Equal(t, []string{"Smith", "Williams"}, indexKeys)
	})

	t.Run("セカンダリキーが同じでプライマリキーが変わる場合、インデックスの value が更新される", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewUniqueIndex("idx_last_name", "last_name", 2)
		bp, metaPageId, _ := InitDisk(t, "users.db")

		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex.MetaPageId = indexMetaPageId

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex})
		err = table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)

		// WHEN: プライマリキーを "a" → "x" に変更、セカンダリキー "Doe" は同じ
		oldRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Doe")}
		newRecord := [][]byte{[]byte("x"), []byte("John"), []byte("Doe")}
		err = table.Update(bp, oldRecord, newRecord)

		// THEN: テーブルが更新されている
		assert.NoError(t, err)
		tree := btree.NewBPlusTree(table.MetaPageId)
		pairs := collectAllTablePairs(t, bp, tree)
		assert.Equal(t, 1, len(pairs))
		assert.Equal(t, [][]byte{[]byte("x")}, pairs[0].key)

		// THEN: ユニークインデックスの value (プライマリキー) が更新されている
		indexTree := btree.NewBPlusTree(table.UniqueIndexes[0].MetaPageId)
		indexIter, err := indexTree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)
		pair, ok, err := indexIter.Next(bp)
		assert.NoError(t, err)
		assert.True(t, ok)

		var decodedKey [][]byte
		Decode(pair.Key, &decodedKey)
		assert.Equal(t, "Doe", string(decodedKey[0]))

		// value はエンコードされた新しいプライマリキー "x"
		var encodedNewPK []byte
		Encode([][]byte{[]byte("x")}, &encodedNewPK)
		assert.Equal(t, encodedNewPK, pair.Value)
	})

	t.Run("プライマリキーを既存のキーに変更するとエラーが返る", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("b"), []byte("Alice")})
		assert.NoError(t, err)

		// WHEN: プライマリキーを "a" → "b" に変更 (既存のキーと衝突)
		oldRecord := [][]byte{[]byte("a"), []byte("John")}
		newRecord := [][]byte{[]byte("b"), []byte("John")}
		err = table.Update(bp, oldRecord, newRecord)

		// THEN: Delete("a") は成功するが Insert("b") が重複キーエラーで失敗する
		assert.Error(t, err)
	})

	t.Run("ユニークインデックスの更新が失敗した場合にエラーが返る", func(t *testing.T) {
		// GIVEN: セカンダリキーが重複する状況を作る
		uniqueIndex := NewUniqueIndex("idx_last_name", "last_name", 2)
		bp, metaPageId, _ := InitDisk(t, "users.db")

		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex.MetaPageId = indexMetaPageId

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex})
		err = table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)

		// WHEN: セカンダリキーを "Doe" → "Smith" に変更 (既存のインデックスキーと衝突)
		oldRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Doe")}
		newRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Smith")}
		err = table.Update(bp, oldRecord, newRecord)

		// THEN: ユニークインデックスの更新でエラーが返る
		assert.Error(t, err)
	})

	t.Run("複数のユニークインデックスがある場合、すべて更新される", func(t *testing.T) {
		// GIVEN: 2 つのユニークインデックスを持つテーブル
		uniqueIndex1 := NewUniqueIndex("idx_first_name", "first_name", 1)
		uniqueIndex2 := NewUniqueIndex("idx_last_name", "last_name", 2)
		bp, metaPageId, _ := InitDisk(t, "users.db")

		indexMetaPageId1, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex1.MetaPageId = indexMetaPageId1
		indexMetaPageId2, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex2.MetaPageId = indexMetaPageId2

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex1, uniqueIndex2})
		err = table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)

		// WHEN: 両方のセカンダリキーが変わる更新
		oldRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Doe")}
		newRecord := [][]byte{[]byte("a"), []byte("Jane"), []byte("Williams")}
		err = table.Update(bp, oldRecord, newRecord)
		assert.NoError(t, err)

		// THEN: idx_first_name が更新されている
		indexTree1 := btree.NewBPlusTree(table.UniqueIndexes[0].MetaPageId)
		iter1, err := indexTree1.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)
		var firstNameKeys []string
		for {
			pair, ok, err := iter1.Next(bp)
			assert.NoError(t, err)
			if !ok {
				break
			}
			var decodedKey [][]byte
			Decode(pair.Key, &decodedKey)
			firstNameKeys = append(firstNameKeys, string(decodedKey[0]))
		}
		assert.Equal(t, []string{"Alice", "Jane"}, firstNameKeys)

		// THEN: idx_last_name が更新されている
		indexTree2 := btree.NewBPlusTree(table.UniqueIndexes[1].MetaPageId)
		iter2, err := indexTree2.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)
		var lastNameKeys []string
		for {
			pair, ok, err := iter2.Next(bp)
			assert.NoError(t, err)
			if !ok {
				break
			}
			var decodedKey [][]byte
			Decode(pair.Key, &decodedKey)
			lastNameKeys = append(lastNameKeys, string(decodedKey[0]))
		}
		assert.Equal(t, []string{"Smith", "Williams"}, lastNameKeys)
	})

	t.Run("存在しないキーを更新するとエラーが返る", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)

		// WHEN: 存在しないキー "z" で更新
		oldRecord := [][]byte{[]byte("z"), []byte("Unknown")}
		newRecord := [][]byte{[]byte("z"), []byte("Updated")}
		err = table.Update(bp, oldRecord, newRecord)

		// THEN
		assert.Error(t, err)
	})
}

// テーブルの全ペアをデコードして収集するヘルパー
type decodedPair struct {
	key   [][]byte
	value [][]byte
}

func collectAllTablePairs(t *testing.T, bp *bufferpool.BufferPool, tree *btree.BPlusTree) []decodedPair {
	t.Helper()
	iter, err := tree.Search(bp, btree.SearchModeStart{})
	assert.NoError(t, err)

	var pairs []decodedPair
	for {
		pair, ok, err := iter.Next(bp)
		assert.NoError(t, err)
		if !ok {
			break
		}
		var decodedKey [][]byte
		var decodedValue [][]byte
		Decode(pair.Key, &decodedKey)
		Decode(pair.Value, &decodedValue)
		pairs = append(pairs, decodedPair{key: decodedKey, value: decodedValue})
	}
	return pairs
}

func TestGetUniqueIndexByName(t *testing.T) {
	t.Run("インデックス名からユニークインデックスを取得できる", func(t *testing.T) {
		// GIVEN
		uniqueIndex1 := NewUniqueIndex("idx_first_name", "first_name", 1)
		uniqueIndex2 := NewUniqueIndex("idx_last_name", "last_name", 2)
		table := NewTable("users", page.PageId{}, 1, []*UniqueIndex{uniqueIndex1, uniqueIndex2})

		// WHEN
		ui, err := table.GetUniqueIndexByName("idx_last_name")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uniqueIndex2, ui)
	})

	t.Run("存在しないインデックス名を指定するとエラーになる", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewUniqueIndex("idx_first_name", "first_name", 1)
		table := NewTable("users", page.PageId{}, 1, []*UniqueIndex{uniqueIndex})

		// WHEN
		ui, err := table.GetUniqueIndexByName("idx_last_name")
		// THEN
		assert.Nil(t, ui)
		assert.Error(t, err)
	})
}

func InitDisk(t *testing.T, pathname string) (bufferPool *bufferpool.BufferPool, metaPageId page.PageId, tmpdir string) {
	tmpdir = t.TempDir()
	filePath := filepath.Join(tmpdir, pathname)

	bp := bufferpool.NewBufferPool(10)
	fileId := bp.AllocateFileId()
	dm, err := disk.NewDisk(fileId, filePath)
	assert.NoError(t, err)
	bp.RegisterDisk(fileId, dm)

	// metaPageId を割り当て
	metaPageId, err = bp.AllocatePageId(fileId)
	assert.NoError(t, err)

	return bp, metaPageId, tmpdir
}
