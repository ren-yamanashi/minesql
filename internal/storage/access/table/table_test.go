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

		table := Table{
			MetaPageId:      disk.PageId(0),
			PrimaryKeyCount: 1,
		}

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
	})
}
