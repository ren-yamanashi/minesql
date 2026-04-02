package handler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnalyzeTable(t *testing.T) {
	t.Run("テーブルの統計情報を収集できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("users", 1, nil, []ColumnParam{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
		})
		assert.NoError(t, err)

		meta, _ := h.Catalog.GetTableMetaByName("users")
		tbl, _ := meta.GetTable()
		err = tbl.Insert(h.BufferPool, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		err = tbl.Insert(h.BufferPool, [][]byte{[]byte("2"), []byte("Bob")})
		assert.NoError(t, err)

		// WHEN
		stats, err := h.AnalyzeTable(meta)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), stats.RecordCount)
		assert.Equal(t, uint64(1), stats.LeafPageCount)
		assert.Equal(t, uint64(2), stats.ColStats["id"].UniqueValues)
		assert.Equal(t, uint64(2), stats.ColStats["name"].UniqueValues)
	})

	t.Run("空のテーブルでもレコード数 0 の統計情報が返る", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("empty", 1, nil, []ColumnParam{
			{Name: "id", Type: ColumnTypeString},
		})
		assert.NoError(t, err)
		meta, _ := h.Catalog.GetTableMetaByName("empty")

		// WHEN
		stats, err := h.AnalyzeTable(meta)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), stats.RecordCount)
	})
}
