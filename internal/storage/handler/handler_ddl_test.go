package handler

import (
	"minesql/internal/storage/lock"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateTable(t *testing.T) {
	t.Run("テーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// WHEN
		err := h.CreateTable("users", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
		})

		// THEN
		assert.NoError(t, err)
		meta, ok := h.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)
		assert.Equal(t, "users", meta.Name)
		assert.Equal(t, uint8(2), meta.NCols)
		assert.Equal(t, uint8(1), meta.PKCount)
	})

	t.Run("ユニークインデックス付きのテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// WHEN
		err := h.CreateTable("users", 1,
			[]CreateIndexParam{
				{Name: "idx_email", ColName: "email", ColIdx: 1, Unique: true},
			},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "email", Type: ColumnTypeString},
			},
		)

		// THEN
		assert.NoError(t, err)
		meta, ok := h.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)
		assert.Equal(t, 1, len(meta.Indexes))
		assert.Equal(t, "idx_email", meta.Indexes[0].Name)
	})

	t.Run("非ユニークインデックス付きのテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// WHEN
		err := h.CreateTable("products", 1,
			[]CreateIndexParam{
				{Name: "idx_category", ColName: "category", ColIdx: 1, Unique: false},
			},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "category", Type: ColumnTypeString},
			},
		)

		// THEN
		assert.NoError(t, err)
		meta, ok := h.Catalog.GetTableMetaByName("products")
		assert.True(t, ok)
		assert.Equal(t, 1, len(meta.Indexes))
		assert.Equal(t, "idx_category", meta.Indexes[0].Name)
		assert.Equal(t, "non-unique secondary", string(meta.Indexes[0].Type))
	})

	t.Run("ユニークと非ユニークのインデックスを同時に作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// WHEN
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
		)

		// THEN
		assert.NoError(t, err)
		meta, ok := h.Catalog.GetTableMetaByName("products")
		assert.True(t, ok)
		assert.Equal(t, 2, len(meta.Indexes))
		assert.Equal(t, "unique secondary", string(meta.Indexes[0].Type))
		assert.Equal(t, "non-unique secondary", string(meta.Indexes[1].Type))
	})

	t.Run("非ユニークインデックス付きテーブルに同一キーの複数レコードを挿入できる", func(t *testing.T) {
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
		)
		assert.NoError(t, err)

		tbl, err := h.GetTable("products")
		assert.NoError(t, err)

		// WHEN: 同一カテゴリの複数レコードを挿入
		err = tbl.Insert(h.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("1"), []byte("Fruit")})
		assert.NoError(t, err)
		err = tbl.Insert(h.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("2"), []byte("Fruit")})
		assert.NoError(t, err)
		err = tbl.Insert(h.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("3"), []byte("Veggie")})

		// THEN: ユニーク制約エラーにならない
		assert.NoError(t, err)
	})

	t.Run("作成したテーブルにレコードを挿入できる", func(t *testing.T) {
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

		tbl, err := h.GetTable("users")
		assert.NoError(t, err)

		// WHEN
		err = tbl.Insert(h.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("1"), []byte("Alice")})

		// THEN
		assert.NoError(t, err)
	})
}
