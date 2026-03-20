package catalog

import (
	"encoding/binary"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/memcomparable"
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetColByName(t *testing.T) {
	t.Run("指定したカラム名のインデックスを取得できる", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
			NewColumnMetadata(1, "email", 2, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(1, "users", 3, 1, colMeta, []*IndexMetadata{}, page.NewPageId(page.FileId(1), 0))

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
		colMeta := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(1, "users", 2, 1, colMeta, []*IndexMetadata{}, page.NewPageId(page.FileId(1), 0))
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
		colMeta := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "email", 1, ColumnTypeString),
		}
		idxMeta := []*IndexMetadata{
			NewIndexMetadata(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
		}
		tableMeta := NewTableMetadata(1, "users", 2, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

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
		colMeta := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "email", 1, ColumnTypeString),
			NewColumnMetadata(1, "username", 2, ColumnTypeString),
		}
		idxMeta := []*IndexMetadata{
			NewIndexMetadata(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
			NewIndexMetadata(1, "idx_username", "username", IndexTypeUnique, page.NewPageId(page.FileId(1), 2)),
		}
		tableMeta := NewTableMetadata(1, "users", 3, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

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
		colMeta := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
			NewColumnMetadata(1, "email", 2, ColumnTypeString),
		}
		idxMeta := []*IndexMetadata{
			NewIndexMetadata(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
		}
		tableMeta := NewTableMetadata(1, "users", 3, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		idx, found := tableMeta.GetIndexByColName("name")

		// THEN
		assert.False(t, found)
		assert.Nil(t, idx)
	})

	t.Run("存在しないカラムを指定した場合、false を返す", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
		}
		idxMeta := []*IndexMetadata{
			NewIndexMetadata(1, "idx_id", "id", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
		}
		tableMeta := NewTableMetadata(1, "users", 1, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		idx, found := tableMeta.GetIndexByColName("non_existent_column")

		// THEN
		assert.False(t, found)
		assert.Nil(t, idx)
	})

	t.Run("インデックスが空のテーブルの場合、false を返す", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(1, "users", 2, 1, colMeta, []*IndexMetadata{}, page.NewPageId(page.FileId(1), 0))
		// WHEN
		idx, found := tableMeta.GetIndexByColName("name")

		// THEN
		assert.False(t, found)
		assert.Nil(t, idx)
	})
}

func TestGetTable(t *testing.T) {
	t.Run("インデックスなしのテーブルを取得できる", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(1, "users", 2, 1, colMeta, []*IndexMetadata{}, page.NewPageId(page.FileId(1), 0))
		// WHEN
		tbl, err := tableMeta.GetTable()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, tbl)
		assert.Equal(t, "users", tbl.Name)
		assert.Equal(t, uint8(1), tbl.PrimaryKeyCount)
		assert.Equal(t, 0, len(tbl.UniqueIndexes))
		assert.Equal(t, page.NewPageId(page.FileId(1), 0), tbl.MetaPageId)
	})

	t.Run("ユニークインデックス付きのテーブルを取得できる", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "email", 1, ColumnTypeString),
		}
		idxMeta := []*IndexMetadata{
			NewIndexMetadata(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
		}
		tableMeta := NewTableMetadata(1, "users", 2, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		tbl, err := tableMeta.GetTable()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, tbl)
		assert.Equal(t, "users", tbl.Name)
		assert.Equal(t, 1, len(tbl.UniqueIndexes))
		assert.Equal(t, "idx_email", tbl.UniqueIndexes[0].Name)
		assert.Equal(t, "email", tbl.UniqueIndexes[0].ColName)
		assert.Equal(t, uint16(1), tbl.UniqueIndexes[0].SecondaryKeyIdx)
		assert.Equal(t, page.NewPageId(page.FileId(1), 1), tbl.UniqueIndexes[0].MetaPageId)
	})

	t.Run("複数のユニークインデックス付きのテーブルを取得できる", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "email", 1, ColumnTypeString),
			NewColumnMetadata(1, "username", 2, ColumnTypeString),
		}
		idxMeta := []*IndexMetadata{
			NewIndexMetadata(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
			NewIndexMetadata(1, "idx_username", "username", IndexTypeUnique, page.NewPageId(page.FileId(1), 2)),
		}
		tableMeta := NewTableMetadata(1, "users", 3, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		tbl, err := tableMeta.GetTable()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, tbl)
		assert.Equal(t, 2, len(tbl.UniqueIndexes))
		assert.Equal(t, "idx_email", tbl.UniqueIndexes[0].Name)
		assert.Equal(t, "email", tbl.UniqueIndexes[0].ColName)
		assert.Equal(t, uint16(1), tbl.UniqueIndexes[0].SecondaryKeyIdx)
		assert.Equal(t, "idx_username", tbl.UniqueIndexes[1].Name)
		assert.Equal(t, "username", tbl.UniqueIndexes[1].ColName)
		assert.Equal(t, uint16(2), tbl.UniqueIndexes[1].SecondaryKeyIdx)
	})

	t.Run("存在しないカラム名を指定したインデックスがある場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
		}
		idxMeta := []*IndexMetadata{
			NewIndexMetadata(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
		}
		tableMeta := NewTableMetadata(1, "users", 2, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		tbl, err := tableMeta.GetTable()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, tbl)
		assert.Contains(t, err.Error(), "column email not found in table users")
	})
}

func TestTableMetadata_Insert(t *testing.T) {
	t.Run("テーブルメタデータを B+Tree に挿入できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		// テーブルメタデータ用の B+Tree を作成
		metaPageId, err := bp.AllocatePageId(page.FileId(0))
		assert.NoError(t, err)
		_, err = btree.CreateBPlusTree(bp, metaPageId)
		assert.NoError(t, err)

		dataMetaPageId := page.NewPageId(page.FileId(1), 0)
		tableMeta := NewTableMetadata(42, "users", 3, 1, []*ColumnMetadata{
			NewColumnMetadata(42, "id", 0, ColumnTypeString),
			NewColumnMetadata(42, "name", 1, ColumnTypeString),
			NewColumnMetadata(42, "email", 2, ColumnTypeString),
		}, []*IndexMetadata{}, dataMetaPageId)
		tableMeta.MetaPageId = metaPageId

		// WHEN
		err = tableMeta.Insert(bp)

		// THEN: B+Tree から挿入したデータを検索して確認
		assert.NoError(t, err)

		btr := btree.NewBPlusTree(metaPageId)
		iter, err := btr.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		pair, ok := iter.Get()
		assert.True(t, ok)

		// key (FileId) をデコード
		var keyParts [][]byte
		memcomparable.Decode(pair.Key, &keyParts)
		tableId := binary.BigEndian.Uint32(keyParts[0])
		assert.Equal(t, uint32(42), tableId)

		// value (Name, NCols, PrimaryKeyCount, DataMetaPageId) をデコード
		var valueParts [][]byte
		memcomparable.Decode(pair.Value, &valueParts)
		assert.Equal(t, "users", string(valueParts[0]))
		assert.Equal(t, uint64(3), binary.BigEndian.Uint64(valueParts[1]))
		assert.Equal(t, uint64(1), binary.BigEndian.Uint64(valueParts[2]))
		assert.Equal(t, dataMetaPageId, page.RestorePageIdFromBytes(valueParts[3]))
	})
}

func TestLoadTableMetadata(t *testing.T) {
	t.Run("テーブルメタデータを読み込める", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		dataMetaPageId := page.NewPageId(page.FileId(1), 0)
		colMeta := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(1, "users", 2, 1, colMeta, []*IndexMetadata{}, dataMetaPageId)
		err = cat.Insert(bp, tableMeta)
		assert.NoError(t, err)

		// WHEN
		result, err := loadTableMetadata(bp, cat.TableMetaPageId, cat.IndexMetaPageId, cat.ColumnMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(result))
		assert.Equal(t, page.FileId(1), result[0].FileId)
		assert.Equal(t, "users", result[0].Name)
		assert.Equal(t, uint8(2), result[0].NCols)
		assert.Equal(t, uint8(1), result[0].PrimaryKeyCount)
		assert.Equal(t, dataMetaPageId, result[0].DataMetaPageId)
	})

	t.Run("カラムメタデータも含めて読み込める", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		colMeta := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
			NewColumnMetadata(1, "email", 2, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(1, "users", 3, 1, colMeta, []*IndexMetadata{}, page.NewPageId(page.FileId(1), 0))
		err = cat.Insert(bp, tableMeta)
		assert.NoError(t, err)

		// WHEN
		result, err := loadTableMetadata(bp, cat.TableMetaPageId, cat.IndexMetaPageId, cat.ColumnMetaPageId)

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

		colMeta := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "email", 1, ColumnTypeString),
		}
		idxDataPageId := page.NewPageId(page.FileId(1), 5)
		idxMeta := []*IndexMetadata{
			NewIndexMetadata(1, "idx_email", "email", IndexTypeUnique, idxDataPageId),
		}
		tableMeta := NewTableMetadata(1, "users", 2, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))
		err = cat.Insert(bp, tableMeta)
		assert.NoError(t, err)

		// WHEN
		result, err := loadTableMetadata(bp, cat.TableMetaPageId, cat.IndexMetaPageId, cat.ColumnMetaPageId)

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

		table1 := NewTableMetadata(1, "users", 2, 1, []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
		}, []*IndexMetadata{}, page.NewPageId(page.FileId(1), 0))
		err = cat.Insert(bp, table1)
		assert.NoError(t, err)

		table2 := NewTableMetadata(2, "posts", 3, 1, []*ColumnMetadata{
			NewColumnMetadata(2, "id", 0, ColumnTypeString),
			NewColumnMetadata(2, "title", 1, ColumnTypeString),
			NewColumnMetadata(2, "body", 2, ColumnTypeString),
		}, []*IndexMetadata{}, page.NewPageId(page.FileId(2), 0))
		err = cat.Insert(bp, table2)
		assert.NoError(t, err)

		// WHEN
		result, err := loadTableMetadata(bp, cat.TableMetaPageId, cat.IndexMetaPageId, cat.ColumnMetaPageId)

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
		result, err := loadTableMetadata(bp, cat.TableMetaPageId, cat.IndexMetaPageId, cat.ColumnMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, result)
	})
}
