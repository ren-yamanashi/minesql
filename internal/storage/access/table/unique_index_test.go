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
func TestUniqueIndex(t *testing.T) {
	t.Run("ユニークインデックスの作成ができ、そのインデックスに値が挿入できる", func(t *testing.T) {
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")

		dm, _ := disk.NewDiskManager(path)
		bpm := bufferpool.NewBufferPoolManager(dm, 10)
		uniqueIndex := NewUniqueIndex(disk.OldPageId(0), 0)

		// WHEN: ユニークインデックスを作成
		err := uniqueIndex.Create(bpm)
		assert.NoError(t, err)

		// WHEN: インデックスに値を挿入
		err = uniqueIndex.Insert(bpm, []uint8{0}, [][]byte{[]byte("John")})
		err = uniqueIndex.Insert(bpm, []uint8{1}, [][]byte{[]byte("Alice")})
		err = uniqueIndex.Insert(bpm, []uint8{2}, [][]byte{[]byte("Eve")})
		err = uniqueIndex.Insert(bpm, []uint8{3}, [][]byte{[]byte("Bob")})
		assert.NoError(t, err)

		// THEN: 挿入したデータが B+Tree に存在する
		tree := btree.NewBTree(uniqueIndex.MetaPageId)
		iter, err := tree.Search(bpm, btree.SearchModeStart{})
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
			iter.Next(bpm)
		}
	})
}
