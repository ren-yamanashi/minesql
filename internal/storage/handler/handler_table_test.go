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
		}, nil)
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
				{Name: "idx_email", ColName: "email", ColIdx: 1, Unique: true},
			},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "email", Type: ColumnTypeString},
			},
			nil,
		)
		assert.NoError(t, err)

		// WHEN
		tbl, err := h.GetTable("users")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(tbl.SecondaryIndexes))
		assert.Equal(t, "idx_email", tbl.SecondaryIndexes[0].Name)
		assert.Equal(t, "email", tbl.SecondaryIndexes[0].ColName)
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
				{Name: "idx_email", ColName: "email", ColIdx: 1, Unique: true},
				{Name: "idx_username", ColName: "username", ColIdx: 2, Unique: true},
			},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "email", Type: ColumnTypeString},
				{Name: "username", Type: ColumnTypeString},
			},
			nil,
		)
		assert.NoError(t, err)

		// WHEN
		tbl, err := h.GetTable("users")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, len(tbl.SecondaryIndexes))
		assert.Equal(t, "idx_email", tbl.SecondaryIndexes[0].Name)
		assert.Equal(t, "idx_username", tbl.SecondaryIndexes[1].Name)
	})

	t.Run("非ユニークインデックス付きテーブルのインデックスが構築される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("products", 1,
			[]CreateIndexParam{
				{Name: "idx_category", ColName: "category", ColIdx: 1, Unique: false},
			},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "category", Type: ColumnTypeString},
			},
			nil,
		)
		assert.NoError(t, err)

		// WHEN
		tbl, err := h.GetTable("products")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(tbl.SecondaryIndexes))
		assert.Equal(t, "idx_category", tbl.SecondaryIndexes[0].Name)
		assert.False(t, tbl.SecondaryIndexes[0].Unique)
	})

	t.Run("複数の非ユニークインデックスが構築される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("products", 1,
			[]CreateIndexParam{
				{Name: "idx_category", ColName: "category", ColIdx: 1, Unique: false},
				{Name: "idx_brand", ColName: "brand", ColIdx: 2, Unique: false},
			},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "category", Type: ColumnTypeString},
				{Name: "brand", Type: ColumnTypeString},
			},
			nil,
		)
		assert.NoError(t, err)

		// WHEN
		tbl, err := h.GetTable("products")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, len(tbl.SecondaryIndexes))
		assert.Equal(t, "idx_category", tbl.SecondaryIndexes[0].Name)
		assert.False(t, tbl.SecondaryIndexes[0].Unique)
		assert.Equal(t, "idx_brand", tbl.SecondaryIndexes[1].Name)
		assert.False(t, tbl.SecondaryIndexes[1].Unique)
	})

	t.Run("ユニークと非ユニークのインデックスが混在して構築される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("products", 1,
			[]CreateIndexParam{
				{Name: "idx_name", ColName: "name", ColIdx: 1, Unique: true},
				{Name: "idx_category", ColName: "category", ColIdx: 2, Unique: false},
			},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "name", Type: ColumnTypeString},
				{Name: "category", Type: ColumnTypeString},
			},
			nil,
		)
		assert.NoError(t, err)

		// WHEN
		tbl, err := h.GetTable("products")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, len(tbl.SecondaryIndexes))
		assert.Equal(t, "idx_name", tbl.SecondaryIndexes[0].Name)
		assert.True(t, tbl.SecondaryIndexes[0].Unique)
		assert.Equal(t, "idx_category", tbl.SecondaryIndexes[1].Name)
		assert.False(t, tbl.SecondaryIndexes[1].Unique)
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

func TestBuildTable(t *testing.T) {
	t.Run("メタデータから Table が構築される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("users", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
		}, nil)
		assert.NoError(t, err)
		tblMeta, ok := h.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)

		// WHEN
		tbl, err := buildTable(tblMeta, h.undoLog, h.redoLog)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "users", tbl.Name)
		assert.Equal(t, uint8(1), tbl.PrimaryKeyCount)
	})

	t.Run("ユニークインデックス付きの Table が構築される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("users", 1,
			[]CreateIndexParam{{Name: "idx_email", ColName: "email", ColIdx: 1, Unique: true}},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "email", Type: ColumnTypeString},
			},
			nil,
		)
		assert.NoError(t, err)
		tblMeta, ok := h.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)

		// WHEN
		tbl, err := buildTable(tblMeta, h.undoLog, h.redoLog)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(tbl.SecondaryIndexes))
		assert.Equal(t, "idx_email", tbl.SecondaryIndexes[0].Name)
	})
}

func TestBuildAllTables(t *testing.T) {
	t.Run("カタログの全テーブルが構築される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("users", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
		}, nil)
		assert.NoError(t, err)
		err = h.CreateTable("orders", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
		}, nil)
		assert.NoError(t, err)

		// WHEN
		tables := buildAllTables(h.Catalog, h.undoLog, h.redoLog)

		// THEN
		assert.Equal(t, 2, len(tables))
		names := []string{tables[0].Name, tables[1].Name}
		assert.Contains(t, names, "users")
		assert.Contains(t, names, "orders")
	})

	t.Run("テーブルが存在しない場合は空を返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// WHEN
		tables := buildAllTables(h.Catalog, h.undoLog, h.redoLog)

		// THEN
		assert.Equal(t, 0, len(tables))
	})
}
