package dictionary

import (
	"encoding/binary"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/encode"
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSortedCols(t *testing.T) {
	t.Run("Pos の順序でソートされたカラムメタデータを返す", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "email", 2, ColumnTypeString),
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMeta(1, "users", 3, 1, colMeta, []*IndexMeta{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		sorted := tableMeta.GetSortedCols()

		// THEN
		assert.Equal(t, 3, len(sorted))
		assert.Equal(t, "id", sorted[0].Name)
		assert.Equal(t, uint16(0), sorted[0].Pos)
		assert.Equal(t, "name", sorted[1].Name)
		assert.Equal(t, uint16(1), sorted[1].Pos)
		assert.Equal(t, "email", sorted[2].Name)
		assert.Equal(t, uint16(2), sorted[2].Pos)
	})

	t.Run("元々 Pos の順序通りのカラムメタデータでも正しく返す", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMeta(1, "users", 2, 1, colMeta, []*IndexMeta{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		sorted := tableMeta.GetSortedCols()

		// THEN
		assert.Equal(t, 2, len(sorted))
		assert.Equal(t, "id", sorted[0].Name)
		assert.Equal(t, "name", sorted[1].Name)
	})

	t.Run("カラムが 1 つだけの場合", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
		}
		tableMeta := NewTableMeta(1, "users", 1, 1, colMeta, []*IndexMeta{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		sorted := tableMeta.GetSortedCols()

		// THEN
		assert.Equal(t, 1, len(sorted))
		assert.Equal(t, "id", sorted[0].Name)
	})

	t.Run("カラムが空の場合", func(t *testing.T) {
		// GIVEN
		tableMeta := NewTableMeta(1, "users", 0, 0, []*ColumnMeta{}, []*IndexMeta{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		sorted := tableMeta.GetSortedCols()

		// THEN
		assert.Equal(t, 0, len(sorted))
	})
}

func TestGetColByName(t *testing.T) {
	t.Run("指定したカラム名のインデックスを取得できる", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "name", 1, ColumnTypeString),
			NewColumnMeta(1, "email", 2, ColumnTypeString),
		}
		tableMeta := NewTableMeta(1, "users", 3, 1, colMeta, []*IndexMeta{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		col, found := tableMeta.GetColByName("name")

		// THEN
		assert.True(t, found)
		assert.Equal(t, uint16(1), col.Pos)
		assert.Equal(t, "name", col.Name)
		assert.Equal(t, ColumnTypeString, col.Type)
	})

	t.Run("存在しないカラム名を指定した場合、false を返す", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMeta(1, "users", 2, 1, colMeta, []*IndexMeta{}, page.NewPageId(page.FileId(1), 0))
		// WHEN
		col, found := tableMeta.GetColByName("non_existent_column")

		// THEN
		assert.False(t, found)
		assert.Nil(t, col)
	})
}

func TestGetIndexByColName(t *testing.T) {
	t.Run("指定したカラム名のインデックスメタデータを取得できる", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "email", 1, ColumnTypeString),
		}
		idxMeta := []*IndexMeta{
			NewIndexMeta(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
		}
		tableMeta := NewTableMeta(1, "users", 2, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		idx, found := tableMeta.GetIndexByColName("email")

		// THEN
		assert.True(t, found)
		assert.NotNil(t, idx)
		assert.Equal(t, "idx_email", idx.Name)
		assert.Equal(t, "email", idx.ColName)
		assert.Equal(t, IndexTypeUnique, idx.Type)
	})

	t.Run("複数のインデックスから指定したカラムのインデックスを取得できる", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "email", 1, ColumnTypeString),
			NewColumnMeta(1, "username", 2, ColumnTypeString),
		}
		idxMeta := []*IndexMeta{
			NewIndexMeta(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
			NewIndexMeta(1, "idx_username", "username", IndexTypeUnique, page.NewPageId(page.FileId(1), 2)),
		}
		tableMeta := NewTableMeta(1, "users", 3, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		idx, found := tableMeta.GetIndexByColName("username")

		// THEN
		assert.True(t, found)
		assert.NotNil(t, idx)
		assert.Equal(t, "idx_username", idx.Name)
		assert.Equal(t, "username", idx.ColName)
	})

	t.Run("インデックスが設定されていないカラムを指定した場合、false を返す", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "name", 1, ColumnTypeString),
			NewColumnMeta(1, "email", 2, ColumnTypeString),
		}
		idxMeta := []*IndexMeta{
			NewIndexMeta(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
		}
		tableMeta := NewTableMeta(1, "users", 3, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		idx, found := tableMeta.GetIndexByColName("name")

		// THEN
		assert.False(t, found)
		assert.Nil(t, idx)
	})

	t.Run("存在しないカラムを指定した場合、false を返す", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
		}
		idxMeta := []*IndexMeta{
			NewIndexMeta(1, "idx_id", "id", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
		}
		tableMeta := NewTableMeta(1, "users", 1, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		idx, found := tableMeta.GetIndexByColName("non_existent_column")

		// THEN
		assert.False(t, found)
		assert.Nil(t, idx)
	})

	t.Run("インデックスが空のテーブルの場合、false を返す", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMeta(1, "users", 2, 1, colMeta, []*IndexMeta{}, page.NewPageId(page.FileId(1), 0))
		// WHEN
		idx, found := tableMeta.GetIndexByColName("name")

		// THEN
		assert.False(t, found)
		assert.Nil(t, idx)
	})
}

func TestTableMeta_Insert(t *testing.T) {
	t.Run("テーブルメタデータを B+Tree に挿入できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		// テーブルメタデータ用の B+Tree を作成
		metaPageId, err := bp.AllocatePageId(page.FileId(0))
		assert.NoError(t, err)
		_, err = btree.CreateBTree(bp, metaPageId)
		assert.NoError(t, err)

		dataMetaPageId := page.NewPageId(page.FileId(1), 0)
		tableMeta := NewTableMeta(42, "users", 3, 1, []*ColumnMeta{
			NewColumnMeta(42, "id", 0, ColumnTypeString),
			NewColumnMeta(42, "name", 1, ColumnTypeString),
			NewColumnMeta(42, "email", 2, ColumnTypeString),
		}, []*IndexMeta{}, dataMetaPageId)
		tableMeta.MetaPageId = metaPageId

		// WHEN
		err = tableMeta.Insert(bp)

		// THEN: B+Tree から挿入したデータを検索して確認
		assert.NoError(t, err)

		btr := btree.NewBTree(metaPageId)
		iter, err := btr.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		record, ok := iter.Get()
		assert.True(t, ok)

		// key (FileId) をデコード
		var keyParts [][]byte
		encode.Decode(record.KeyBytes(), &keyParts)
		tableId := binary.BigEndian.Uint32(keyParts[0])
		assert.Equal(t, uint32(42), tableId)

		// value (Name, NCols, PrimaryKeyCount, DataMetaPageId) をデコード
		var valueParts [][]byte
		encode.Decode(record.NonKeyBytes(), &valueParts)
		assert.Equal(t, "users", string(valueParts[0]))
		assert.Equal(t, uint64(3), binary.BigEndian.Uint64(valueParts[1]))
		assert.Equal(t, uint64(1), binary.BigEndian.Uint64(valueParts[2]))
		assert.Equal(t, dataMetaPageId, page.RestorePageIdFromBytes(valueParts[3]))
	})
}

func TestLoadTableMeta(t *testing.T) {
	t.Run("テーブルメタデータを読み込める", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		dataMetaPageId := page.NewPageId(page.FileId(1), 0)
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMeta(1, "users", 2, 1, colMeta, []*IndexMeta{}, dataMetaPageId)
		err = cat.Insert(bp, tableMeta)
		assert.NoError(t, err)

		// WHEN
		result, err := loadTableMeta(bp, cat.TableMetaPageId, cat.IndexMetaPageId, cat.ColumnMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(result))
		assert.Equal(t, page.FileId(1), result[0].FileId)
		assert.Equal(t, "users", result[0].Name)
		assert.Equal(t, uint8(2), result[0].NCols)
		assert.Equal(t, uint8(1), result[0].PKCount)
		assert.Equal(t, dataMetaPageId, result[0].DataMetaPageId)
	})

	t.Run("カラムメタデータも含めて読み込める", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "name", 1, ColumnTypeString),
			NewColumnMeta(1, "email", 2, ColumnTypeString),
		}
		tableMeta := NewTableMeta(1, "users", 3, 1, colMeta, []*IndexMeta{}, page.NewPageId(page.FileId(1), 0))
		err = cat.Insert(bp, tableMeta)
		assert.NoError(t, err)

		// WHEN
		result, err := loadTableMeta(bp, cat.TableMetaPageId, cat.IndexMetaPageId, cat.ColumnMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(result))
		assert.Equal(t, 3, len(result[0].Cols))
		assert.Equal(t, "id", result[0].Cols[0].Name)
		assert.Equal(t, "name", result[0].Cols[1].Name)
		assert.Equal(t, "email", result[0].Cols[2].Name)
	})

	t.Run("インデックスメタデータも含めて読み込める", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "email", 1, ColumnTypeString),
		}
		idxDataPageId := page.NewPageId(page.FileId(1), 5)
		idxMeta := []*IndexMeta{
			NewIndexMeta(1, "idx_email", "email", IndexTypeUnique, idxDataPageId),
		}
		tableMeta := NewTableMeta(1, "users", 2, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))
		err = cat.Insert(bp, tableMeta)
		assert.NoError(t, err)

		// WHEN
		result, err := loadTableMeta(bp, cat.TableMetaPageId, cat.IndexMetaPageId, cat.ColumnMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(result))
		assert.Equal(t, 1, len(result[0].Indexes))
		assert.Equal(t, "idx_email", result[0].Indexes[0].Name)
		assert.Equal(t, "email", result[0].Indexes[0].ColName)
		assert.Equal(t, IndexTypeUnique, result[0].Indexes[0].Type)
		assert.Equal(t, idxDataPageId, result[0].Indexes[0].DataMetaPageId)
	})

	t.Run("複数のテーブルメタデータを読み込める", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		table1 := NewTableMeta(1, "users", 2, 1, []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "name", 1, ColumnTypeString),
		}, []*IndexMeta{}, page.NewPageId(page.FileId(1), 0))
		err = cat.Insert(bp, table1)
		assert.NoError(t, err)

		table2 := NewTableMeta(2, "posts", 3, 1, []*ColumnMeta{
			NewColumnMeta(2, "id", 0, ColumnTypeString),
			NewColumnMeta(2, "title", 1, ColumnTypeString),
			NewColumnMeta(2, "body", 2, ColumnTypeString),
		}, []*IndexMeta{}, page.NewPageId(page.FileId(2), 0))
		err = cat.Insert(bp, table2)
		assert.NoError(t, err)

		// WHEN
		result, err := loadTableMeta(bp, cat.TableMetaPageId, cat.IndexMetaPageId, cat.ColumnMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result))
		assert.Equal(t, "users", result[0].Name)
		assert.Equal(t, uint8(2), result[0].NCols)
		assert.Equal(t, 2, len(result[0].Cols))
		assert.Equal(t, "posts", result[1].Name)
		assert.Equal(t, uint8(3), result[1].NCols)
		assert.Equal(t, 3, len(result[1].Cols))
	})

	t.Run("テーブルが存在しない場合は空のスライスを返す", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// WHEN
		result, err := loadTableMeta(bp, cat.TableMetaPageId, cat.IndexMetaPageId, cat.ColumnMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, result)
	})
}
