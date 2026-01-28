package catalog

import (
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetColIndex(t *testing.T) {
	t.Run("指定したカラム名のインデックスを取得できる", func(t *testing.T) {
		// GIVEN
		colMeta := []ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
			NewColumnMetadata(1, "email", 2, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(1, "users", 3, colMeta, []IndexMetadata{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		idx, found := tableMeta.GetColIndex("name")

		// THEN
		assert.True(t, found)
		assert.Equal(t, 1, idx)
	})

	t.Run("先頭のカラム名のインデックスを取得できる", func(t *testing.T) {
		// GIVEN
		colMeta := []ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(1, "users", 2, colMeta, []IndexMetadata{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		idx, found := tableMeta.GetColIndex("id")

		// THEN
		assert.True(t, found)
		assert.Equal(t, 0, idx)
	})

	t.Run("末尾のカラム名のインデックスを取得できる", func(t *testing.T) {
		// GIVEN
		colMeta := []ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
			NewColumnMetadata(1, "email", 2, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(1, "users", 3, colMeta, []IndexMetadata{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		idx, found := tableMeta.GetColIndex("email")

		// THEN
		assert.True(t, found)
		assert.Equal(t, 2, idx)
	})

	t.Run("存在しないカラム名を指定した場合、false を返す", func(t *testing.T) {
		// GIVEN
		colMeta := []ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(1, "users", 2, colMeta, []IndexMetadata{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		idx, found := tableMeta.GetColIndex("non_existent_column")

		// THEN
		assert.False(t, found)
		assert.Equal(t, -1, idx)
	})

	t.Run("カラムが空のテーブルの場合、false を返す", func(t *testing.T) {
		// GIVEN
		tableMeta := NewTableMetadata(1, "empty_table", 0, []ColumnMetadata{}, []IndexMetadata{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		idx, found := tableMeta.GetColIndex("any_column")

		// THEN
		assert.False(t, found)
		assert.Equal(t, -1, idx)
	})
}

func TestHasColumn(t *testing.T) {
	t.Run("指定したカラムが存在する場合、true を返す", func(t *testing.T) {
		// GIVEN
		colMeta := []ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
			NewColumnMetadata(1, "email", 2, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(1, "users", 3, colMeta, []IndexMetadata{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		result := tableMeta.HasColumn("name")

		// THEN
		assert.True(t, result)
	})

	t.Run("先頭のカラムが存在することを確認できる", func(t *testing.T) {
		// GIVEN
		colMeta := []ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(1, "users", 2, colMeta, []IndexMetadata{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		result := tableMeta.HasColumn("id")

		// THEN
		assert.True(t, result)
	})

	t.Run("末尾のカラムが存在することを確認できる", func(t *testing.T) {
		// GIVEN
		colMeta := []ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
			NewColumnMetadata(1, "email", 2, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(1, "users", 3, colMeta, []IndexMetadata{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		result := tableMeta.HasColumn("email")

		// THEN
		assert.True(t, result)
	})

	t.Run("存在しないカラムを指定した場合、false を返す", func(t *testing.T) {
		// GIVEN
		colMeta := []ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(1, "users", 2, colMeta, []IndexMetadata{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		result := tableMeta.HasColumn("non_existent_column")

		// THEN
		assert.False(t, result)
	})

	t.Run("カラムが空のテーブルの場合、false を返す", func(t *testing.T) {
		// GIVEN
		tableMeta := NewTableMetadata(1, "empty_table", 0, []ColumnMetadata{}, []IndexMetadata{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		result := tableMeta.HasColumn("any_column")

		// THEN
		assert.False(t, result)
	})
}

func TestGetIndexByColName(t *testing.T) {
	t.Run("指定したカラム名のインデックスメタデータを取得できる", func(t *testing.T) {
		// GIVEN
		colMeta := []ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "email", 1, ColumnTypeString),
		}
		idxMeta := []IndexMetadata{
			NewIndexMetadata(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
		}
		tableMeta := NewTableMetadata(1, "users", 2, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

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
		colMeta := []ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "email", 1, ColumnTypeString),
			NewColumnMetadata(1, "username", 2, ColumnTypeString),
		}
		idxMeta := []IndexMetadata{
			NewIndexMetadata(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
			NewIndexMetadata(1, "idx_username", "username", IndexTypeUnique, page.NewPageId(page.FileId(1), 2)),
		}
		tableMeta := NewTableMetadata(1, "users", 3, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

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
		colMeta := []ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
			NewColumnMetadata(1, "email", 2, ColumnTypeString),
		}
		idxMeta := []IndexMetadata{
			NewIndexMetadata(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
		}
		tableMeta := NewTableMetadata(1, "users", 3, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		idx, found := tableMeta.GetIndexByColName("name")

		// THEN
		assert.False(t, found)
		assert.Nil(t, idx)
	})

	t.Run("存在しないカラムを指定した場合、false を返す", func(t *testing.T) {
		// GIVEN
		colMeta := []ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
		}
		idxMeta := []IndexMetadata{
			NewIndexMetadata(1, "idx_id", "id", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
		}
		tableMeta := NewTableMetadata(1, "users", 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		idx, found := tableMeta.GetIndexByColName("non_existent_column")

		// THEN
		assert.False(t, found)
		assert.Nil(t, idx)
	})

	t.Run("インデックスが空のテーブルの場合、false を返す", func(t *testing.T) {
		// GIVEN
		colMeta := []ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMetadata(1, "users", 2, colMeta, []IndexMetadata{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		idx, found := tableMeta.GetIndexByColName("name")

		// THEN
		assert.False(t, found)
		assert.Nil(t, idx)
	})
}
