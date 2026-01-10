package table

import (
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Create と Insert をテスト
func TestTable(t *testing.T) {
	t.Run("テーブルの作成ができ、そのテーブルに値が挿入できる", func(t *testing.T) {
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")

		dm, _ := disk.NewDiskManager(path)
		bpm := bufferpool.NewBufferPoolManager(dm, 10)
		uniqueIndexes := NewUniqueIndex(disk.OLD_INVALID_PAGE_ID, 2)
		table := NewTable(disk.OldPageId(0), 1, []*UniqueIndex{uniqueIndexes})

		// WHEN: テーブルを作成
		err := table.Create(bpm)
		assert.NoError(t, err)

		// WHEN: 行を挿入
		err = table.Insert(bpm, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		err = table.Insert(bpm, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		err = table.Insert(bpm, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
		err = table.Insert(bpm, [][]byte{[]byte("d"), []byte("Eve"), []byte("Davis")})
		assert.NoError(t, err)

		// THEN: 挿入したデータが B+Tree に存在する
		tree := btree.NewBTree(table.MetaPageId)
		iter, err := tree.Search(bpm, btree.SearchModeStart{})
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
			iter.Next(bpm)
		}
		assert.Equal(t, len(expectedRecords), i)

		// THEN: ユニークインデックスにもデータが挿入されている
		uniqueIndexTree := btree.NewBTree(table.UniqueIndexes[0].MetaPageId)
		uniqueIndexIter, err := uniqueIndexTree.Search(bpm, btree.SearchModeStart{})
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
			uniqueIndexIter.Next(bpm)
		}
		assert.Equal(t, len(expectedUniqueIndexRecords), j)
	})
}
