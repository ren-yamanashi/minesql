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
				{Name: "idx_email", ColName: "email", UkIdx: 1},
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
