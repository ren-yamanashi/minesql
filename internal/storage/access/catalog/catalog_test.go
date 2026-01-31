package catalog

import (
	"encoding/binary"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
	"minesql/internal/storage/page"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateCatalog(t *testing.T) {
	t.Run("新しいカタログを作成できる", func(t *testing.T) {
		// GIVEN
		bpm, tmpdir := InitCatalogDiskManager(t)
		defer os.RemoveAll(tmpdir)

		// WHEN
		cat, err := CreateCatalog(bpm)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cat)
		assert.Equal(t, page.FileId(0), cat.TableMetaPageId.FileId)
		assert.Equal(t, page.FileId(0), cat.IndexMetaPageId.FileId)
		assert.Equal(t, page.FileId(0), cat.ColumnMetaPageId.FileId)
		assert.Equal(t, uint64(0), cat.NextTableId)
		assert.Empty(t, cat.metadata)
	})

	t.Run("カタログのヘッダーページにマジックナンバーが設定される", func(t *testing.T) {
		// GIVEN
		bpm, tmpdir := InitCatalogDiskManager(t)
		defer os.RemoveAll(tmpdir)

		// WHEN
		_, err := CreateCatalog(bpm)
		assert.NoError(t, err)

		// THEN: ヘッダーページを読み込んでマジックナンバーを確認
		headerPageId := page.NewPageId(page.FileId(0), 0)
		headerPage, err := bpm.FetchPage(headerPageId)
		assert.NoError(t, err)
		defer bpm.UnRefPage(headerPageId)

		data := headerPage.GetReadData()
		assert.Equal(t, "MINE", string(data[0:4]))
	})
}

func TestAllocateTableId(t *testing.T) {
	t.Run("テーブルIDを順番に採番できる", func(t *testing.T) {
		// GIVEN
		bpm, tmpdir := InitCatalogDiskManager(t)
		defer os.RemoveAll(tmpdir)

		cat, err := CreateCatalog(bpm)
		assert.NoError(t, err)

		// WHEN: テーブルIDを複数回採番
		id1, err := cat.AllocateTableId(bpm)
		assert.NoError(t, err)
		id2, err := cat.AllocateTableId(bpm)
		assert.NoError(t, err)
		id3, err := cat.AllocateTableId(bpm)
		assert.NoError(t, err)

		// THEN: 順番に採番される
		assert.Equal(t, uint64(0), id1)
		assert.Equal(t, uint64(1), id2)
		assert.Equal(t, uint64(2), id3)
		assert.Equal(t, uint64(3), cat.NextTableId)
	})

	t.Run("採番後のテーブルIDがディスクに保存される", func(t *testing.T) {
		// GIVEN
		bpm, tmpdir := InitCatalogDiskManager(t)
		defer os.RemoveAll(tmpdir)

		cat, err := CreateCatalog(bpm)
		assert.NoError(t, err)

		// WHEN: テーブルIDを採番
		_, err = cat.AllocateTableId(bpm)
		assert.NoError(t, err)

		// THEN: ヘッダーページから NextTableId が読み取れる
		headerPageId := page.NewPageId(page.FileId(0), 0)
		headerPage, err := bpm.FetchPage(headerPageId)
		assert.NoError(t, err)
		defer bpm.UnRefPage(headerPageId)

		data := headerPage.GetReadData()
		savedNextTableId := binary.BigEndian.Uint64(data[16:24])
		assert.Equal(t, uint64(1), savedNextTableId)
	})
}

func TestInsert(t *testing.T) {
	t.Run("テーブルメタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		bpm, tmpdir := InitCatalogDiskManager(t)
		defer os.RemoveAll(tmpdir)

		cat, err := CreateCatalog(bpm)
		assert.NoError(t, err)

		tableId := uint64(1)
		metaPageId := page.NewPageId(page.FileId(1), 0)
		colMeta := []ColumnMetadata{
			NewColumnMetadata(tableId, "id", 0, ColumnTypeString),
			NewColumnMetadata(tableId, "name", 1, ColumnTypeString),
		}
		idxMeta := []IndexMetadata{}
		tableMeta := NewTableMetadata(tableId, "users", 2, 1, colMeta, idxMeta, metaPageId)

		// WHEN
		err = cat.Insert(bpm, tableMeta)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cat.metadata))
		assert.Equal(t, tableMeta, cat.metadata[0])
	})

	t.Run("カラムメタデータ付きのテーブルメタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		bpm, tmpdir := InitCatalogDiskManager(t)
		defer os.RemoveAll(tmpdir)

		cat, err := CreateCatalog(bpm)
		assert.NoError(t, err)

		tableId := uint64(1)
		metaPageId := page.NewPageId(page.FileId(1), 0)
		colMeta := []ColumnMetadata{
			NewColumnMetadata(tableId, "id", 0, ColumnTypeString),
			NewColumnMetadata(tableId, "name", 1, ColumnTypeString),
			NewColumnMetadata(tableId, "email", 2, ColumnTypeString),
		}
		idxMeta := []IndexMetadata{}
		tableMeta := NewTableMetadata(tableId, "users", 3, 1, colMeta, idxMeta, metaPageId)

		// WHEN
		err = cat.Insert(bpm, tableMeta)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cat.metadata))
		assert.Equal(t, uint8(3), cat.metadata[0].NCols)
		assert.Equal(t, 3, len(cat.metadata[0].Cols))
	})

	t.Run("インデックスメタデータ付きのテーブルメタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		bpm, tmpdir := InitCatalogDiskManager(t)
		defer os.RemoveAll(tmpdir)

		cat, err := CreateCatalog(bpm)
		assert.NoError(t, err)

		tableId := uint64(1)
		metaPageId := page.NewPageId(page.FileId(1), 0)
		indexMetaPageId := page.NewPageId(page.FileId(1), 1)
		colMeta := []ColumnMetadata{
			NewColumnMetadata(tableId, "id", 0, ColumnTypeString),
			NewColumnMetadata(tableId, "email", 1, ColumnTypeString),
		}
		idxMeta := []IndexMetadata{
			NewIndexMetadata(tableId, "idx_email", "email", IndexTypeUnique, indexMetaPageId),
		}
		tableMeta := NewTableMetadata(tableId, "users", 2, 1, colMeta, idxMeta, metaPageId)

		// WHEN
		err = cat.Insert(bpm, tableMeta)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cat.metadata))
		assert.Equal(t, 1, len(cat.metadata[0].Indexes))
		assert.Equal(t, "idx_email", cat.metadata[0].Indexes[0].Name)
	})
}

func TestGetTableMetadataByName(t *testing.T) {
	t.Run("テーブル名からテーブルメタデータを取得できる", func(t *testing.T) {
		// GIVEN
		bpm, tmpdir := InitCatalogDiskManager(t)
		defer os.RemoveAll(tmpdir)

		cat, err := CreateCatalog(bpm)
		assert.NoError(t, err)

		tableId := uint64(1)
		metaPageId := page.NewPageId(page.FileId(1), 0)
		colMeta := []ColumnMetadata{
			NewColumnMetadata(tableId, "id", 0, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(tableId, "users", 1, 1, colMeta, []IndexMetadata{}, metaPageId)
		err = cat.Insert(bpm, tableMeta)
		assert.NoError(t, err)

		// WHEN
		result, err := cat.GetTableMetadataByName("users")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "users", result.Name)
		assert.Equal(t, tableId, result.TableId)
	})

	t.Run("存在しないテーブル名を指定するとエラーを返す", func(t *testing.T) {
		// GIVEN
		bpm, tmpdir := InitCatalogDiskManager(t)
		defer os.RemoveAll(tmpdir)

		cat, err := CreateCatalog(bpm)
		assert.NoError(t, err)

		// WHEN
		result, err := cat.GetTableMetadataByName("non_existent")

		// THEN
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("複数のテーブルから正しいテーブルを取得できる", func(t *testing.T) {
		// GIVEN
		bpm, tmpdir := InitCatalogDiskManager(t)
		defer os.RemoveAll(tmpdir)

		cat, err := CreateCatalog(bpm)
		assert.NoError(t, err)

		// 複数のテーブルを挿入
		table1Meta := NewTableMetadata(1, "users", 1, 1, []ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
		}, []IndexMetadata{}, page.NewPageId(page.FileId(1), 0))
		table2Meta := NewTableMetadata(2, "posts", 1, 1, []ColumnMetadata{
			NewColumnMetadata(2, "id", 0, ColumnTypeString),
		}, []IndexMetadata{}, page.NewPageId(page.FileId(2), 0))
		table3Meta := NewTableMetadata(3, "comments", 1, 1, []ColumnMetadata{
			NewColumnMetadata(3, "id", 0, ColumnTypeString),
		}, []IndexMetadata{}, page.NewPageId(page.FileId(3), 0))

		err = cat.Insert(bpm, table1Meta)
		assert.NoError(t, err)
		err = cat.Insert(bpm, table2Meta)
		assert.NoError(t, err)
		err = cat.Insert(bpm, table3Meta)
		assert.NoError(t, err)

		// WHEN
		result, err := cat.GetTableMetadataByName("posts")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "posts", result.Name)
		assert.Equal(t, uint64(2), result.TableId)
	})
}

func InitCatalogDiskManager(t *testing.T) (bpm *bufferpool.BufferPoolManager, tmpdir string) {
	tmpdir = t.TempDir()
	filePath := filepath.Join(tmpdir, "minesql.db")

	bpm = bufferpool.NewBufferPoolManager(10)
	fileId := page.FileId(0)
	dm, err := disk.NewDiskManager(fileId, filePath)
	assert.NoError(t, err)
	bpm.RegisterDiskManager(fileId, dm)

	return bpm, tmpdir
}
