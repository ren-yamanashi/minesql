package executor

import (
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewCreateTable(t *testing.T) {
	t.Run("インデックスとカラムのパラメータが nil の場合に空のスライスに変換される", func(t *testing.T) {
		// WHEN
		createTable := NewCreateTable("users", 1, nil, nil)

		// THEN
		assert.NotNil(t, createTable.indexParams)
		assert.NotNil(t, createTable.columnParams)
		assert.Equal(t, 0, len(createTable.indexParams))
		assert.Equal(t, 0, len(createTable.columnParams))
	})
}

func TestCreateTable_Next(t *testing.T) {
	t.Run("テーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()
		e := handler.Get()
		createTable := NewCreateTable("users", 1, nil, nil)

		// WHEN
		_, err := createTable.Next()

		// THEN
		assert.NoError(t, err)
		tblMeta, ok := e.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)
		assert.NotNil(t, tblMeta)
		assert.Equal(t, "users", tblMeta.Name)
		assert.Equal(t, uint8(1), tblMeta.PKCount)
	})

	t.Run("カラムを指定してテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()
		e := handler.Get()
		createTable := NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
			{Name: "email", Type: "string"},
		})

		// WHEN
		_, err := createTable.Next()

		// THEN
		assert.NoError(t, err)
		tblMeta, ok := e.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)
		assert.NotNil(t, tblMeta)
		assert.Equal(t, uint8(3), tblMeta.NCols)
		assert.Equal(t, 3, len(tblMeta.Cols))
		assert.Equal(t, "id", tblMeta.Cols[0].Name)
		assert.Equal(t, "int", string(tblMeta.Cols[0].Type))
		assert.Equal(t, uint16(0), tblMeta.Cols[0].Pos)
		assert.Equal(t, "name", tblMeta.Cols[1].Name)
		assert.Equal(t, "string", string(tblMeta.Cols[1].Type))
		assert.Equal(t, uint16(1), tblMeta.Cols[1].Pos)
		assert.Equal(t, "email", tblMeta.Cols[2].Name)
		assert.Equal(t, "string", string(tblMeta.Cols[2].Type))
		assert.Equal(t, uint16(2), tblMeta.Cols[2].Pos)
	})

	t.Run("ユニークインデックス付きのテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()
		e := handler.Get()
		createTable := NewCreateTable("users", 1, []handler.CreateIndexParam{
			{Name: "email", ColName: "email", UkIdx: 1},
		}, nil)

		// WHEN
		_, err := createTable.Next()
		assert.NoError(t, err)

		// THEN
		tblMeta, ok := e.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)
		assert.NotNil(t, tblMeta)
		assert.Equal(t, 1, len(tblMeta.Indexes))
		assert.Equal(t, "email", tblMeta.Indexes[0].ColName)
	})

	t.Run("テーブルファイルが作成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()
		e := handler.Get()
		createTable := NewCreateTable("users", 1, nil, nil)

		// WHEN
		_, err := createTable.Next()
		assert.NoError(t, err)

		// THEN
		tblMeta, ok := e.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)
		assert.NotNil(t, tblMeta)
		// ディスクマネージャが登録されていることを確認
		dm, dmErr := e.BufferPool.GetDisk(tblMeta.DataMetaPageId.FileId)
		assert.NoError(t, dmErr)
		assert.NotNil(t, dm)
	})
}
