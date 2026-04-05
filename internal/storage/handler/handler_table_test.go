package handler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTable(t *testing.T) {
	t.Run("テーブル名からテーブルを取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("users", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
		})
		assert.NoError(t, err)

		// WHEN
		tbl, err := h.GetTable("users")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, tbl)
		assert.Equal(t, "users", tbl.Name)
		assert.Equal(t, uint8(1), tbl.PrimaryKeyCount)
	})

	t.Run("ユニークインデックス付きテーブルのインデックスが構築される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("users", 1,
			[]CreateIndexParam{
				{Name: "idx_email", ColName: "email", UkIdx: 1},
			},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "email", Type: ColumnTypeString},
			},
		)
		assert.NoError(t, err)

		// WHEN
		tbl, err := h.GetTable("users")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(tbl.UniqueIndexes))
		assert.Equal(t, "idx_email", tbl.UniqueIndexes[0].Name)
		assert.Equal(t, "email", tbl.UniqueIndexes[0].ColName)
	})

	t.Run("複数のユニークインデックスが構築される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("users", 1,
			[]CreateIndexParam{
				{Name: "idx_email", ColName: "email", UkIdx: 1},
				{Name: "idx_username", ColName: "username", UkIdx: 2},
			},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "email", Type: ColumnTypeString},
				{Name: "username", Type: ColumnTypeString},
			},
		)
		assert.NoError(t, err)

		// WHEN
		tbl, err := h.GetTable("users")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, len(tbl.UniqueIndexes))
		assert.Equal(t, "idx_email", tbl.UniqueIndexes[0].Name)
		assert.Equal(t, "idx_username", tbl.UniqueIndexes[1].Name)
	})

	t.Run("存在しないテーブル名を指定するとエラーを返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// WHEN
		tbl, err := h.GetTable("non_existent")

		// THEN
		assert.Error(t, err)
		assert.Nil(t, tbl)
		assert.Contains(t, err.Error(), "table non_existent not found")
	})
}
