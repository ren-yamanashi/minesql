package executor

import (
	"minesql/internal/storage"
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateTable(t *testing.T) {
	t.Run("テーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		storage.ResetStorageManager()
		storage.InitStorageManager()
		sm := storage.GetStorageManager()
		createTable := NewCreateTable("users", 1, nil, nil)

		// WHEN
		_, err := createTable.Next()

		// THEN
		assert.NoError(t, err)
		tblMeta, err := sm.Catalog.GetTableMetadataByName("users")
		assert.NotNil(t, tblMeta)
		assert.Equal(t, "users", tblMeta.Name)
		assert.Equal(t, uint8(1), tblMeta.PrimaryKeyCount)
	})

	t.Run("カラムを指定してテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		storage.ResetStorageManager()
		storage.InitStorageManager()
		sm := storage.GetStorageManager()
		createTable := NewCreateTable("users", 1, nil, []*ColumnParam{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
			{Name: "email", Type: "string"},
		})

		// WHEN
		_, err := createTable.Next()

		// THEN
		assert.NoError(t, err)
		tblMeta, err := sm.Catalog.GetTableMetadataByName("users")
		assert.NoError(t, err)
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
		storage.ResetStorageManager()
		storage.InitStorageManager()
		sm := storage.GetStorageManager()
		createTable := NewCreateTable("users", 1, []*IndexParam{
			{Name: "email", ColName: "email", SecondaryKey: 1},
		}, nil)

		// WHEN
		_, err := createTable.Next()

		// THEN
		tblMeta, err := sm.Catalog.GetTableMetadataByName("users")
		assert.NoError(t, err)
		assert.NotNil(t, tblMeta)
		assert.Equal(t, 1, len(tblMeta.Indexes))
		assert.Equal(t, "email", tblMeta.Indexes[0].ColName)
	})

	t.Run("テーブルファイルが作成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		storage.ResetStorageManager()
		storage.InitStorageManager()
		sm := storage.GetStorageManager()
		createTable := NewCreateTable("users", 1, nil, nil)

		// WHEN
		_, err := createTable.Next()

		// THEN
		tblMeta, err := sm.Catalog.GetTableMetadataByName("users")
		assert.NoError(t, err)
		// FileId が採番されていることを確認
		assert.NotEqual(t, page.FileId(0), tblMeta.DataMetaPageId.FileId)
		// ディスクマネージャが登録されていることを確認
		dm, dmErr := sm.BufferPoolManager.GetDiskManager(tblMeta.DataMetaPageId.FileId)
		assert.NoError(t, dmErr)
		assert.NotNil(t, dm)
	})
}
