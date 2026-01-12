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
		engine := storage.GetStorageManager()
		createTable := NewCreateTable("users", 1, nil)

		// WHEN
		_, err := createTable.Next()

		// THEN
		assert.NoError(t, err)
		tbl, err := engine.GetTable("users")
		assert.NoError(t, err)
		assert.NotNil(t, tbl)
		assert.Equal(t, "users", tbl.Name)
		assert.Equal(t, 1, tbl.PrimaryKeyCount)
	})

	t.Run("ユニークインデックス付きのテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		storage.ResetStorageManager()
		storage.InitStorageManager()
		engine := storage.GetStorageManager()
		createTable := NewCreateTable("users", 1, []*IndexParam{
			{Name: "email", SecondaryKey: 1},
		})

		// WHEN
		_, err := createTable.Next()

		// THEN
		assert.NoError(t, err)
		tbl, err := engine.GetTable("users")
		assert.NoError(t, err)
		assert.NotNil(t, tbl)
		assert.Equal(t, 1, len(tbl.UniqueIndexes))
		assert.Equal(t, "email", tbl.UniqueIndexes[0].Name)
	})

	t.Run("テーブルファイルが作成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		storage.ResetStorageManager()
		storage.InitStorageManager()
		engine := storage.GetStorageManager()
		createTable := NewCreateTable("users", 1, nil)

		// WHEN
		_, err := createTable.Next()

		// THEN
		assert.NoError(t, err)
		tbl, err := engine.GetTable("users")
		assert.NoError(t, err)
		// FileId が採番されていることを確認
		assert.NotEqual(t, page.FileId(0), tbl.MetaPageId.FileId)
		// ディスクマネージャが登録されていることを確認
		bpm := engine.GetBufferPoolManager()
		dm, dmErr := bpm.GetDiskManager(tbl.MetaPageId.FileId)
		assert.NoError(t, dmErr)
		assert.NotNil(t, dm)
	})
}
