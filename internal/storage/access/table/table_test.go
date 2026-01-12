package table

import (
	"minesql/internal/storage/access/btree"
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
		uniqueIndex := NewUniqueIndex("last_name", 2)
		bpm, metaPageId, _ := InitDiskManager(t, "users.db")

		// UniqueIndex の metaPageId を割り当て
		indexMetaPageId, err := bpm.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex.MetaPageId = indexMetaPageId

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex})

		// WHEN
		err = table.Create(bpm)
		assert.NoError(t, err)

		// WHEN
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

	t.Run("テーブルとそのインデックスが同じディスクファイル (同じ FileId) に保存される", func(t *testing.T) {
		// GIVEN
		// 2つのインデックスを持つテーブルを作成
		uniqueIndex1 := NewUniqueIndex("first_name", 1)
		uniqueIndex2 := NewUniqueIndex("last_name", 2)
		bpm, metaPageId, tmpdir := InitDiskManager(t, "users.db")

		// UniqueIndex の metaPageId を割り当て
		indexMetaPageId1, err := bpm.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex1.MetaPageId = indexMetaPageId1
		indexMetaPageId2, err := bpm.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex2.MetaPageId = indexMetaPageId2

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex1, uniqueIndex2})

		// WHEN
		err = table.Create(bpm)

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

func TestGetUniqueIndexByName(t *testing.T) {
	t.Run("インデックス名からユニークインデックスを取得できる", func(t *testing.T) {
		// GIVEN
		uniqueIndex1 := NewUniqueIndex("first_name", 1)
		uniqueIndex2 := NewUniqueIndex("last_name", 2)
		table := NewTable("users", page.PageId{}, 1, []*UniqueIndex{uniqueIndex1, uniqueIndex2})

		// WHEN
		ui, err := table.GetUniqueIndexByName("last_name")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uniqueIndex2, ui)
	})

	t.Run("存在しないインデックス名を指定するとエラーになる", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewUniqueIndex("first_name", 1)
		table := NewTable("users", page.PageId{}, 1, []*UniqueIndex{uniqueIndex})

		// WHEN
		ui, err := table.GetUniqueIndexByName("last_name")

		// THEN
		assert.Nil(t, ui)
		assert.Error(t, err)
	})
}

func InitDiskManager(t *testing.T, pathname string) (bufferpoolManager *bufferpool.BufferPoolManager, metaPageId page.PageId, tmpdir string) {
	tmpdir = t.TempDir()
	filePath := filepath.Join(tmpdir, pathname)

	bpm := bufferpool.NewBufferPoolManager(10)
	fileId := bpm.AllocateFileId()
	dm, err := disk.NewDiskManager(fileId, filePath)
	assert.NoError(t, err)
	bpm.RegisterDiskManager(fileId, dm)

	// metaPageId を割り当て
	metaPageId, err = bpm.AllocatePageId(fileId)
	assert.NoError(t, err)

	return bpm, metaPageId, tmpdir
}
