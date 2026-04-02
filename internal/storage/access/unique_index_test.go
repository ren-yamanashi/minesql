package access

import (
	"fmt"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/encode"
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
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetapageId, 0, 1)

		// WHEN: ユニークインデックスを作成
		err = uniqueIndex.Create(bp)
		assert.NoError(t, err)

		// WHEN: インデックスに値を挿入 (Key = concat(encodedSecondaryKey, encodedPK), NonKey = nil)
		// encodedPK は memcomparable エンコード済みのバイト列を渡す
		var encodedPK0, encodedPK1, encodedPK2, encodedPK3 []byte
		encode.Encode([][]byte{[]byte("pk0")}, &encodedPK0)
		encode.Encode([][]byte{[]byte("pk1")}, &encodedPK1)
		encode.Encode([][]byte{[]byte("pk2")}, &encodedPK2)
		encode.Encode([][]byte{[]byte("pk3")}, &encodedPK3)

		err = uniqueIndex.Insert(bp, encodedPK0, [][]byte{[]byte("John")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, encodedPK1, [][]byte{[]byte("Alice")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, encodedPK2, [][]byte{[]byte("Eve")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, encodedPK3, [][]byte{[]byte("Bob")})
		assert.NoError(t, err)

		// THEN: 挿入したデータが B+Tree に存在する
		tree := btree.NewBTree(uniqueIndex.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		// Key = concat(encodedSecondaryKey, encodedPK) なのでセカンダリキーでソートされる
		expectedRecords := []struct {
			secondaryKey string
			pk           string
		}{
			{"Alice", "pk1"},
			{"Bob", "pk3"},
			{"Eve", "pk2"},
			{"John", "pk0"},
		}

		i := 0
		for {
			record, ok := iter.Get()
			if !ok {
				break
			}
			expected := expectedRecords[i]

			// Key をデコードしてセカンダリキーと PK を分離
			var keyColumns [][]byte
			encode.Decode(record.KeyBytes(), &keyColumns)

			assert.Equal(t, expected.secondaryKey, string(keyColumns[0]))
			assert.Equal(t, expected.pk, string(keyColumns[1]))

			// Header は DeleteMark=0
			assert.Equal(t, uint8(0), record.HeaderBytes()[0])
			// NonKey は nil (空)
			assert.Equal(t, 0, len(record.NonKeyBytes()))

			i++
			_, _, err := iter.Next(bp)
			assert.NoError(t, err)
		}
		assert.Equal(t, len(expectedRecords), i)
	})
}

func TestUniqueIndexDelete(t *testing.T) {
	t.Run("ユニークインデックスからソフトデリートできる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "test.db")

		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetapageId, 0, 1)

		err = uniqueIndex.Create(bp)
		assert.NoError(t, err)

		var encodedPK0, encodedPK1, encodedPK2 []byte
		encode.Encode([][]byte{[]byte("pk0")}, &encodedPK0)
		encode.Encode([][]byte{[]byte("pk1")}, &encodedPK1)
		encode.Encode([][]byte{[]byte("pk2")}, &encodedPK2)

		err = uniqueIndex.Insert(bp, encodedPK0, [][]byte{[]byte("John")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, encodedPK1, [][]byte{[]byte("Alice")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, encodedPK2, [][]byte{[]byte("Eve")})
		assert.NoError(t, err)

		// WHEN: "Alice" をソフトデリート
		err = uniqueIndex.SoftDelete(bp, encodedPK1, [][]byte{[]byte("Alice")})

		// THEN: ソフトデリートが成功する
		assert.NoError(t, err)

		// B+Tree を直接走査すると、3 件すべて存在するが "Alice" の DeleteMark が 1
		tree := btree.NewBTree(uniqueIndex.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		type indexEntry struct {
			secondaryKey string
			deleteMark   uint8
		}
		var entries []indexEntry
		for {
			record, ok := iter.Get()
			if !ok {
				break
			}
			var keyColumns [][]byte
			encode.Decode(record.KeyBytes(), &keyColumns)
			entries = append(entries, indexEntry{
				secondaryKey: string(keyColumns[0]),
				deleteMark:   record.HeaderBytes()[0],
			})
			_, _, err := iter.Next(bp)
			assert.NoError(t, err)
		}

		assert.Equal(t, 3, len(entries))
		assert.Equal(t, "Alice", entries[0].secondaryKey)
		assert.Equal(t, uint8(1), entries[0].deleteMark) // ソフトデリート済み
		assert.Equal(t, "Eve", entries[1].secondaryKey)
		assert.Equal(t, uint8(0), entries[1].deleteMark)
		assert.Equal(t, "John", entries[2].secondaryKey)
		assert.Equal(t, uint8(0), entries[2].deleteMark)
	})

	t.Run("ソフトデリート後に同じセカンダリキーで再挿入できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "test.db")

		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetapageId, 0, 1)

		err = uniqueIndex.Create(bp)
		assert.NoError(t, err)

		var encodedPK0 []byte
		encode.Encode([][]byte{[]byte("pk0")}, &encodedPK0)

		err = uniqueIndex.Insert(bp, encodedPK0, [][]byte{[]byte("John")})
		assert.NoError(t, err)

		// WHEN: ソフトデリート後に同じセカンダリキー + 同じ PK で再挿入
		err = uniqueIndex.SoftDelete(bp, encodedPK0, [][]byte{[]byte("John")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, encodedPK0, [][]byte{[]byte("John")})
		assert.NoError(t, err)

		// THEN: active なレコードとして存在する
		tree := btree.NewBTree(uniqueIndex.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		record, ok := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, uint8(0), record.HeaderBytes()[0]) // active
	})

	t.Run("ユニークインデックスから物理削除できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "test.db")

		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetapageId, 0, 1)

		err = uniqueIndex.Create(bp)
		assert.NoError(t, err)

		var encodedPK0, encodedPK1, encodedPK2 []byte
		encode.Encode([][]byte{[]byte("pk0")}, &encodedPK0)
		encode.Encode([][]byte{[]byte("pk1")}, &encodedPK1)
		encode.Encode([][]byte{[]byte("pk2")}, &encodedPK2)

		err = uniqueIndex.Insert(bp, encodedPK0, [][]byte{[]byte("John")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, encodedPK1, [][]byte{[]byte("Alice")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, encodedPK2, [][]byte{[]byte("Eve")})
		assert.NoError(t, err)

		// WHEN: "Alice" を物理削除
		err = uniqueIndex.Delete(bp, encodedPK1, [][]byte{[]byte("Alice")})

		// THEN: 物理削除が成功する
		assert.NoError(t, err)

		// B+Tree を直接走査すると 2 件のみ存在する (物理的に消えている)
		tree := btree.NewBTree(uniqueIndex.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		type indexEntry struct {
			secondaryKey string
			deleteMark   uint8
		}
		var entries []indexEntry
		for {
			record, ok := iter.Get()
			if !ok {
				break
			}
			var keyColumns [][]byte
			encode.Decode(record.KeyBytes(), &keyColumns)
			entries = append(entries, indexEntry{
				secondaryKey: string(keyColumns[0]),
				deleteMark:   record.HeaderBytes()[0],
			})
			_, _, err := iter.Next(bp)
			assert.NoError(t, err)
		}

		assert.Equal(t, 2, len(entries))
		assert.Equal(t, "Eve", entries[0].secondaryKey)
		assert.Equal(t, uint8(0), entries[0].deleteMark)
		assert.Equal(t, "John", entries[1].secondaryKey)
		assert.Equal(t, uint8(0), entries[1].deleteMark)
	})

	t.Run("ユニークインデックスから物理削除した後は同じセカンダリキーで再挿入できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "test.db")

		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetapageId, 0, 1)

		err = uniqueIndex.Create(bp)
		assert.NoError(t, err)

		var encodedPK0 []byte
		encode.Encode([][]byte{[]byte("pk0")}, &encodedPK0)

		err = uniqueIndex.Insert(bp, encodedPK0, [][]byte{[]byte("John")})
		assert.NoError(t, err)

		// WHEN: 物理削除後に同じセカンダリキー + 同じ PK で再挿入
		err = uniqueIndex.Delete(bp, encodedPK0, [][]byte{[]byte("John")})
		assert.NoError(t, err)
		err = uniqueIndex.Insert(bp, encodedPK0, [][]byte{[]byte("John")})
		assert.NoError(t, err)

		// THEN: active なレコードとして存在する
		tree := btree.NewBTree(uniqueIndex.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		record, ok := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, uint8(0), record.HeaderBytes()[0]) // active
	})
}

func TestUniqueIndexConstraint(t *testing.T) {
	t.Run("active なレコードが存在する場合にユニーク制約違反エラーが返る", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "test.db")

		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetapageId, 0, 1)

		err = uniqueIndex.Create(bp)
		assert.NoError(t, err)

		var encodedPK0, encodedPK1 []byte
		encode.Encode([][]byte{[]byte("pk0")}, &encodedPK0)
		encode.Encode([][]byte{[]byte("pk1")}, &encodedPK1)

		err = uniqueIndex.Insert(bp, encodedPK0, [][]byte{[]byte("John")})
		assert.NoError(t, err)

		// WHEN: 同じセカンダリキーで別の PK を挿入 (ユニーク制約違反)
		err = uniqueIndex.Insert(bp, encodedPK1, [][]byte{[]byte("John")})

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("ソフトデリート済みの場合はユニーク制約違反にならない", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "test.db")

		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetapageId, 0, 1)

		err = uniqueIndex.Create(bp)
		assert.NoError(t, err)

		var encodedPK0, encodedPK1 []byte
		encode.Encode([][]byte{[]byte("pk0")}, &encodedPK0)
		encode.Encode([][]byte{[]byte("pk1")}, &encodedPK1)

		err = uniqueIndex.Insert(bp, encodedPK0, [][]byte{[]byte("John")})
		assert.NoError(t, err)

		// ソフトデリート
		err = uniqueIndex.SoftDelete(bp, encodedPK0, [][]byte{[]byte("John")})
		assert.NoError(t, err)

		// WHEN: 同じセカンダリキーで別の PK を挿入
		err = uniqueIndex.Insert(bp, encodedPK1, [][]byte{[]byte("John")})

		// THEN: ユニーク制約違反にならない
		assert.NoError(t, err)
	})

	t.Run("複数のソフトデリート済みエントリがある場合でもユニーク制約に違反しない", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "test.db")
		indexMetapageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetapageId, 0, 1)
		err = uniqueIndex.Create(bp)
		assert.NoError(t, err)

		var encodedPK0, encodedPK1, encodedPK2 []byte
		encode.Encode([][]byte{[]byte("pk0")}, &encodedPK0)
		encode.Encode([][]byte{[]byte("pk1")}, &encodedPK1)
		encode.Encode([][]byte{[]byte("pk2")}, &encodedPK2)

		// 同じセカンダリキーで挿入 → ソフトデリートを 2 回繰り返す
		err = uniqueIndex.Insert(bp, encodedPK0, [][]byte{[]byte("John")})
		assert.NoError(t, err)
		err = uniqueIndex.SoftDelete(bp, encodedPK0, [][]byte{[]byte("John")})
		assert.NoError(t, err)

		err = uniqueIndex.Insert(bp, encodedPK1, [][]byte{[]byte("John")})
		assert.NoError(t, err)
		err = uniqueIndex.SoftDelete(bp, encodedPK1, [][]byte{[]byte("John")})
		assert.NoError(t, err)

		// WHEN: 3 回目の挿入
		err = uniqueIndex.Insert(bp, encodedPK2, [][]byte{[]byte("John")})

		// THEN: ソフトデリート済みのみなのでユニーク制約違反にならない
		assert.NoError(t, err)
	})
}

func TestUniqueIndexLeafPageCount(t *testing.T) {
	t.Run("作成直後のテーブルのリーフページ数は 1", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "test.db")
		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetaPageId, 0, 1)

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
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetaPageId, 0, 1)

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
	t.Run("作成直後のインデックスの高さは 1", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "test.db")
		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetaPageId, 0, 1)
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
		uniqueIndex := NewUniqueIndexAccessMethod("test_index", "test", indexMetaPageId, 0, 1)
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
