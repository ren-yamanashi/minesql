package dictionary

import (
	"encoding/binary"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/file"
	"minesql/internal/storage/page"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewCatalog(t *testing.T) {
	t.Run("既存のカタログを開くと、保存されたメタデータが読み込まれる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		fileId := page.FileId(1)
		metaPageId := page.NewPageId(page.FileId(1), 0)
		colMeta := []*ColumnMeta{
			NewColumnMeta(fileId, "id", 0, ColumnTypeString),
			NewColumnMeta(fileId, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMeta(fileId, "users", 2, 1, colMeta, []*IndexMeta{}, metaPageId)
		err = cat.Insert(bp, tableMeta)
		assert.NoError(t, err)

		// ページをフラッシュ
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// WHEN
		bp2 := buffer.NewBufferPool(10, nil)
		filePath := filepath.Join(tmpdir, "minesql.db")
		catalogFileId := page.FileId(0)
		dm2, err := file.NewDisk(catalogFileId, filePath)
		assert.NoError(t, err)
		bp2.RegisterDisk(catalogFileId, dm2)

		cat2, err := NewCatalog(bp2)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cat2)
		assert.Equal(t, 1, len(cat2.metadata))
		assert.Equal(t, "users", cat2.metadata[0].Name)
		assert.Equal(t, fileId, cat2.metadata[0].FileId)
		assert.Equal(t, uint8(2), cat2.metadata[0].NCols)
	})

	t.Run("カラムメタデータも正しく読み込まれる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		fileId := page.FileId(1)
		metaPageId := page.NewPageId(page.FileId(1), 0)
		colMeta := []*ColumnMeta{
			NewColumnMeta(fileId, "id", 0, ColumnTypeString),
			NewColumnMeta(fileId, "name", 1, ColumnTypeString),
			NewColumnMeta(fileId, "email", 2, ColumnTypeString),
		}
		tableMeta := NewTableMeta(fileId, "users", 3, 1, colMeta, []*IndexMeta{}, metaPageId)
		err = cat.Insert(bp, tableMeta)
		assert.NoError(t, err)

		// ページをフラッシュ
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// WHEN
		bp2 := buffer.NewBufferPool(10, nil)
		filePath := filepath.Join(tmpdir, "minesql.db")
		catalogFileId := page.FileId(0)
		dm2, err := file.NewDisk(catalogFileId, filePath)
		assert.NoError(t, err)
		bp2.RegisterDisk(catalogFileId, dm2)

		cat2, err := NewCatalog(bp2)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cat2.metadata))
		assert.Equal(t, 3, len(cat2.metadata[0].Cols))
		assert.Equal(t, "id", cat2.metadata[0].Cols[0].Name)
		assert.Equal(t, uint16(0), cat2.metadata[0].Cols[0].Pos)
		assert.Equal(t, "name", cat2.metadata[0].Cols[1].Name)
		assert.Equal(t, uint16(1), cat2.metadata[0].Cols[1].Pos)
		assert.Equal(t, "email", cat2.metadata[0].Cols[2].Name)
		assert.Equal(t, uint16(2), cat2.metadata[0].Cols[2].Pos)
	})

	t.Run("インデックスメタデータも正しく読み込まれる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		fileId := page.FileId(1)
		metaPageId := page.NewPageId(page.FileId(1), 0)
		indexMetaPageId := page.NewPageId(page.FileId(1), 1)
		colMeta := []*ColumnMeta{
			NewColumnMeta(fileId, "id", 0, ColumnTypeString),
			NewColumnMeta(fileId, "email", 1, ColumnTypeString),
		}
		idxMeta := []*IndexMeta{
			NewIndexMeta(fileId, "idx_email", "email", IndexTypeUnique, indexMetaPageId),
		}
		tableMeta := NewTableMeta(fileId, "users", 2, 1, colMeta, idxMeta, metaPageId)
		err = cat.Insert(bp, tableMeta)
		assert.NoError(t, err)

		// ページをフラッシュ
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// WHEN
		bp2 := buffer.NewBufferPool(10, nil)
		filePath := filepath.Join(tmpdir, "minesql.db")
		catalogFileId := page.FileId(0)
		dm2, err := file.NewDisk(catalogFileId, filePath)
		assert.NoError(t, err)
		bp2.RegisterDisk(catalogFileId, dm2)

		cat2, err := NewCatalog(bp2)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cat2.metadata))
		assert.Equal(t, 1, len(cat2.metadata[0].Indexes))
		assert.Equal(t, "idx_email", cat2.metadata[0].Indexes[0].Name)
		assert.Equal(t, "email", cat2.metadata[0].Indexes[0].ColName)
		assert.Equal(t, IndexTypeUnique, cat2.metadata[0].Indexes[0].Type)
		assert.Equal(t, indexMetaPageId, cat2.metadata[0].Indexes[0].DataMetaPageId)
	})

	t.Run("制約メタデータも正しく読み込まれる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		fileId := page.FileId(1)
		metaPageId := page.NewPageId(page.FileId(1), 0)
		colMeta := []*ColumnMeta{
			NewColumnMeta(fileId, "id", 0, ColumnTypeString),
			NewColumnMeta(fileId, "user_id", 1, ColumnTypeString),
		}
		conMeta := []*ConstraintMeta{
			NewConstraintMeta(fileId, "id", "PRIMARY", "", ""),
			NewConstraintMeta(fileId, "user_id", "fk_user", "users", "id"),
		}
		tableMeta := NewTableMeta(fileId, "orders", 2, 1, colMeta, []*IndexMeta{}, metaPageId)
		tableMeta.Constraints = conMeta
		err = cat.Insert(bp, tableMeta)
		assert.NoError(t, err)

		// ページをフラッシュ
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// WHEN
		bp2 := buffer.NewBufferPool(10, nil)
		filePath := filepath.Join(tmpdir, "minesql.db")
		catalogFileId := page.FileId(0)
		dm2, err := file.NewDisk(catalogFileId, filePath)
		assert.NoError(t, err)
		bp2.RegisterDisk(catalogFileId, dm2)

		cat2, err := NewCatalog(bp2)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cat2.metadata))
		assert.Equal(t, 2, len(cat2.metadata[0].Constraints))

		// PK 制約
		pkCon := cat2.metadata[0].Constraints[0]
		assert.Equal(t, "id", pkCon.ColName)
		assert.Equal(t, "PRIMARY", pkCon.ConstraintName)
		assert.Equal(t, "", pkCon.RefTableName)
		assert.Equal(t, "", pkCon.RefColName)

		// FK 制約
		fkCon := cat2.metadata[0].Constraints[1]
		assert.Equal(t, "user_id", fkCon.ColName)
		assert.Equal(t, "fk_user", fkCon.ConstraintName)
		assert.Equal(t, "users", fkCon.RefTableName)
		assert.Equal(t, "id", fkCon.RefColName)
	})

	t.Run("複数のテーブルが正しく読み込まれる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// テーブル 1: users
		table1Meta := NewTableMeta(page.FileId(1), "users", 2, 1, []*ColumnMeta{
			NewColumnMeta(page.FileId(1), "id", 0, ColumnTypeString),
			NewColumnMeta(page.FileId(1), "name", 1, ColumnTypeString),
		}, []*IndexMeta{}, page.NewPageId(page.FileId(1), 0))
		err = cat.Insert(bp, table1Meta)
		assert.NoError(t, err)

		// テーブル 2: posts
		table2Meta := NewTableMeta(page.FileId(2), "posts", 3, 1, []*ColumnMeta{
			NewColumnMeta(page.FileId(2), "id", 0, ColumnTypeString),
			NewColumnMeta(page.FileId(2), "title", 1, ColumnTypeString),
			NewColumnMeta(page.FileId(2), "body", 2, ColumnTypeString),
		}, []*IndexMeta{}, page.NewPageId(page.FileId(2), 0))
		err = cat.Insert(bp, table2Meta)
		assert.NoError(t, err)

		// テーブル 3: comments
		table3Meta := NewTableMeta(page.FileId(3), "comments", 2, 1, []*ColumnMeta{
			NewColumnMeta(page.FileId(3), "id", 0, ColumnTypeString),
			NewColumnMeta(page.FileId(3), "text", 1, ColumnTypeString),
		}, []*IndexMeta{}, page.NewPageId(page.FileId(3), 0))
		err = cat.Insert(bp, table3Meta)
		assert.NoError(t, err)

		// ページをフラッシュ
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// WHEN
		bp2 := buffer.NewBufferPool(10, nil)
		filePath := filepath.Join(tmpdir, "minesql.db")
		catalogFileId := page.FileId(0)
		dm2, err := file.NewDisk(catalogFileId, filePath)
		assert.NoError(t, err)
		bp2.RegisterDisk(catalogFileId, dm2)

		cat2, err := NewCatalog(bp2)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 3, len(cat2.metadata))

		// テーブル名で検索して確認
		usersTable, ok := cat2.GetTableMetaByName("users")
		assert.True(t, ok)
		assert.NotNil(t, usersTable)
		assert.Equal(t, "users", usersTable.Name)
		assert.Equal(t, uint8(2), usersTable.NCols)
		assert.Equal(t, 2, len(usersTable.Cols))

		postsTable, ok := cat2.GetTableMetaByName("posts")
		assert.True(t, ok)
		assert.NotNil(t, postsTable)
		assert.Equal(t, "posts", postsTable.Name)
		assert.Equal(t, uint8(3), postsTable.NCols)
		assert.Equal(t, 3, len(postsTable.Cols))

		commentsTable, ok := cat2.GetTableMetaByName("comments")
		assert.True(t, ok)
		assert.NotNil(t, commentsTable)
		assert.Equal(t, "comments", commentsTable.Name)
		assert.Equal(t, uint8(2), commentsTable.NCols)
		assert.Equal(t, 2, len(commentsTable.Cols))
	})

	t.Run("UndoFileId が正しく復元される", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)
		assert.Equal(t, page.FileId(1), cat.UndoFileId)

		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// WHEN
		bp2 := buffer.NewBufferPool(10, nil)
		dm2, err := file.NewDisk(page.FileId(0), filepath.Join(tmpdir, "minesql.db"))
		assert.NoError(t, err)
		bp2.RegisterDisk(page.FileId(0), dm2)

		cat2, err := NewCatalog(bp2)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.FileId(1), cat2.UndoFileId)
	})

	t.Run("NextFileId も正しく復元される", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// FileId を複数回採番
		_, err = cat.AllocateFileId(bp)
		assert.NoError(t, err)
		_, err = cat.AllocateFileId(bp)
		assert.NoError(t, err)
		_, err = cat.AllocateFileId(bp)
		assert.NoError(t, err)

		// ページをフラッシュ
		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// WHEN
		bp2 := buffer.NewBufferPool(10, nil)
		filePath := filepath.Join(tmpdir, "minesql.db")
		catalogFileId := page.FileId(0)
		dm2, err := file.NewDisk(catalogFileId, filePath)
		assert.NoError(t, err)
		bp2.RegisterDisk(catalogFileId, dm2)

		cat2, err := NewCatalog(bp2)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.FileId(5), cat2.NextFileId)

		// 次の採番が正しく動作することを確認
		nextId, err := cat2.AllocateFileId(bp2)
		assert.NoError(t, err)
		assert.Equal(t, page.FileId(5), nextId)
		assert.Equal(t, page.FileId(6), cat2.NextFileId)
	})

	t.Run("マジックナンバーが不正な場合、ErrInvalidCatalogFile を返す", func(t *testing.T) {
		// GIVEN: ヘッダーページにマジックナンバー以外のデータが書き込まれたカタログ
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		headerPageId := page.NewPageId(page.FileId(0), 0)
		err := bp.AddPage(headerPageId)
		assert.NoError(t, err)
		data, err := bp.GetWritePageData(headerPageId)
		assert.NoError(t, err)
		copy(data[0:4], []byte("XXXX"))

		err = bp.FlushAllPages()
		assert.NoError(t, err)

		// WHEN: 新しい BufferPool でカタログを開き直す
		bp2 := buffer.NewBufferPool(10, nil)
		filePath := filepath.Join(tmpdir, "minesql.db")
		dm2, err := file.NewDisk(page.FileId(0), filePath)
		assert.NoError(t, err)
		bp2.RegisterDisk(page.FileId(0), dm2)

		cat, err := NewCatalog(bp2)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidCatalogFile)
		assert.Nil(t, cat)
	})
}

func TestCreateCatalog(t *testing.T) {
	t.Run("新しいカタログを作成できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		// WHEN
		cat, err := CreateCatalog(bp)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cat)
		assert.Equal(t, page.FileId(0), cat.TableMetaPageId.FileId)
		assert.Equal(t, page.FileId(0), cat.IndexMetaPageId.FileId)
		assert.Equal(t, page.FileId(0), cat.ColumnMetaPageId.FileId)
		assert.Equal(t, page.FileId(2), cat.NextFileId)
		assert.Equal(t, page.FileId(1), cat.UndoFileId)
		assert.Empty(t, cat.metadata)
	})

	t.Run("カタログのヘッダーページにマジックナンバーが設定される", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		// WHEN
		_, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// THEN: ヘッダーページを読み込んでマジックナンバーを確認
		headerPageId := page.NewPageId(page.FileId(0), 0)
		data, err := bp.GetReadPageData(headerPageId)
		assert.NoError(t, err)
		defer bp.UnRefPage(headerPageId)

		assert.Equal(t, "MINE", string(data[0:4]))
	})
}

func TestInsert(t *testing.T) {
	t.Run("テーブルメタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		fileId := page.FileId(1)
		metaPageId := page.NewPageId(page.FileId(1), 0)
		colMeta := []*ColumnMeta{
			NewColumnMeta(fileId, "id", 0, ColumnTypeString),
			NewColumnMeta(fileId, "name", 1, ColumnTypeString),
		}
		idxMeta := []*IndexMeta{}
		tableMeta := NewTableMeta(fileId, "users", 2, 1, colMeta, idxMeta, metaPageId)

		// WHEN
		err = cat.Insert(bp, tableMeta)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cat.metadata))
		assert.Equal(t, "users", cat.metadata[0].Name)
		assert.Equal(t, fileId, cat.metadata[0].FileId)
		assert.Equal(t, uint8(2), cat.metadata[0].NCols)
	})

	t.Run("カラムメタデータ付きのテーブルメタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		fileId := page.FileId(1)
		metaPageId := page.NewPageId(page.FileId(1), 0)
		colMeta := []*ColumnMeta{
			NewColumnMeta(fileId, "id", 0, ColumnTypeString),
			NewColumnMeta(fileId, "name", 1, ColumnTypeString),
			NewColumnMeta(fileId, "email", 2, ColumnTypeString),
		}
		idxMeta := []*IndexMeta{}
		tableMeta := NewTableMeta(fileId, "users", 3, 1, colMeta, idxMeta, metaPageId)

		// WHEN
		err = cat.Insert(bp, tableMeta)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cat.metadata))
		assert.Equal(t, uint8(3), cat.metadata[0].NCols)
		assert.Equal(t, 3, len(cat.metadata[0].Cols))
	})

	t.Run("制約メタデータ付きのテーブルメタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		fileId := page.FileId(1)
		metaPageId := page.NewPageId(page.FileId(1), 0)
		colMeta := []*ColumnMeta{
			NewColumnMeta(fileId, "id", 0, ColumnTypeString),
			NewColumnMeta(fileId, "user_id", 1, ColumnTypeString),
		}
		conMeta := []*ConstraintMeta{
			NewConstraintMeta(fileId, "id", "PRIMARY", "", ""),
			NewConstraintMeta(fileId, "user_id", "fk_user", "users", "id"),
		}
		tableMeta := NewTableMeta(fileId, "orders", 2, 1, colMeta, []*IndexMeta{}, metaPageId)
		tableMeta.Constraints = conMeta

		// WHEN
		err = cat.Insert(bp, tableMeta)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cat.metadata))
		assert.Equal(t, 2, len(cat.metadata[0].Constraints))
		assert.Equal(t, "PRIMARY", cat.metadata[0].Constraints[0].ConstraintName)
		assert.Equal(t, "fk_user", cat.metadata[0].Constraints[1].ConstraintName)
		assert.Equal(t, "users", cat.metadata[0].Constraints[1].RefTableName)
		assert.Equal(t, "id", cat.metadata[0].Constraints[1].RefColName)
	})

	t.Run("インデックスメタデータ付きのテーブルメタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		fileId := page.FileId(1)
		metaPageId := page.NewPageId(page.FileId(1), 0)
		indexMetaPageId := page.NewPageId(page.FileId(1), 1)
		colMeta := []*ColumnMeta{
			NewColumnMeta(fileId, "id", 0, ColumnTypeString),
			NewColumnMeta(fileId, "email", 1, ColumnTypeString),
		}
		idxMeta := []*IndexMeta{
			NewIndexMeta(fileId, "idx_email", "email", IndexTypeUnique, indexMetaPageId),
		}
		tableMeta := NewTableMeta(fileId, "users", 2, 1, colMeta, idxMeta, metaPageId)

		// WHEN
		err = cat.Insert(bp, tableMeta)

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
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		fileId := page.FileId(1)
		metaPageId := page.NewPageId(page.FileId(1), 0)
		colMeta := []*ColumnMeta{
			NewColumnMeta(fileId, "id", 0, ColumnTypeString),
		}
		tableMeta := NewTableMeta(fileId, "users", 1, 1, colMeta, []*IndexMeta{}, metaPageId)
		err = cat.Insert(bp, tableMeta)
		assert.NoError(t, err)

		// WHEN
		result, ok := cat.GetTableMetaByName("users")

		// THEN
		assert.True(t, ok)
		assert.NotNil(t, result)
		assert.Equal(t, "users", result.Name)
		assert.Equal(t, fileId, result.FileId)
	})

	t.Run("存在しないテーブル名を指定すると nil を返す", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// WHEN
		result, ok := cat.GetTableMetaByName("non_existent")

		// THEN
		assert.False(t, ok)
		assert.Nil(t, result)
	})

	t.Run("複数のテーブルから正しいテーブルを取得できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// 複数のテーブルを挿入
		table1Meta := NewTableMeta(page.FileId(1), "users", 1, 1, []*ColumnMeta{
			NewColumnMeta(page.FileId(1), "id", 0, ColumnTypeString),
		}, []*IndexMeta{}, page.NewPageId(page.FileId(1), 0))
		table2Meta := NewTableMeta(page.FileId(2), "posts", 1, 1, []*ColumnMeta{
			NewColumnMeta(page.FileId(2), "id", 0, ColumnTypeString),
		}, []*IndexMeta{}, page.NewPageId(page.FileId(2), 0))
		table3Meta := NewTableMeta(page.FileId(3), "comments", 1, 1, []*ColumnMeta{
			NewColumnMeta(page.FileId(3), "id", 0, ColumnTypeString),
		}, []*IndexMeta{}, page.NewPageId(page.FileId(3), 0))

		err = cat.Insert(bp, table1Meta)
		assert.NoError(t, err)
		err = cat.Insert(bp, table2Meta)
		assert.NoError(t, err)
		err = cat.Insert(bp, table3Meta)
		assert.NoError(t, err)

		// WHEN
		result, ok := cat.GetTableMetaByName("posts")

		// THEN
		assert.True(t, ok)
		assert.NotNil(t, result)
		assert.Equal(t, "posts", result.Name)
		assert.Equal(t, page.FileId(2), result.FileId)
	})
}

func TestAllocateFileId(t *testing.T) {
	t.Run("FileId を順番に採番できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// WHEN: FileId を複数回採番
		id1, err := cat.AllocateFileId(bp)
		assert.NoError(t, err)
		id2, err := cat.AllocateFileId(bp)
		assert.NoError(t, err)
		id3, err := cat.AllocateFileId(bp)
		assert.NoError(t, err)

		// THEN: 順番に採番される (FileId(0) はカタログ、FileId(1) は UNDO ログ用なので、テーブル用は 2 から開始)
		assert.Equal(t, page.FileId(2), id1)
		assert.Equal(t, page.FileId(3), id2)
		assert.Equal(t, page.FileId(4), id3)
		assert.Equal(t, page.FileId(5), cat.NextFileId)
	})

	t.Run("採番後の FileId がディスクに保存される", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// WHEN: FileId を採番
		_, err = cat.AllocateFileId(bp)
		assert.NoError(t, err)

		// THEN: ヘッダーページから NextFileId が読み取れる
		headerPageId := page.NewPageId(page.FileId(0), 0)
		data, err := bp.GetReadPageData(headerPageId)
		assert.NoError(t, err)
		defer bp.UnRefPage(headerPageId)
		savedNextFileId := binary.BigEndian.Uint32(data[20:24])
		assert.Equal(t, uint32(3), savedNextFileId)
	})
}

func InitCatalogDisk(t *testing.T) (bp *buffer.BufferPool, tmpdir string) {
	tmpdir = t.TempDir()
	filePath := filepath.Join(tmpdir, "minesql.db")

	bp = buffer.NewBufferPool(10, nil)
	fileId := page.FileId(0)
	dm, err := file.NewDisk(fileId, filePath)
	assert.NoError(t, err)
	bp.RegisterDisk(fileId, dm)

	return bp, tmpdir
}

func removeTmpdir(t *testing.T, tmpdir string) {
	if err := os.RemoveAll(tmpdir); err != nil {
		t.Logf("failed to remove tmpdir: %v", err)
	}
}
