package storage

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/catalog"
	"minesql/internal/storage/page"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitStorageManager(t *testing.T) {
	t.Run("グローバル StorageManager を初期化できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageManager()

		// WHEN
		InitStorageManager()

		// THEN
		assert.NotNil(t, manager)
	})

	t.Run("複数回初期化しても同じインスタンスが返される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageManager()

		// WHEN
		engine1 := InitStorageManager()
		engine2 := InitStorageManager()

		// THEN
		assert.Same(t, engine1, engine2)
	})
}

func TestGetStorageManager(t *testing.T) {
	t.Run("初期化後にグローバル StorageManager を取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageManager()
		InitStorageManager()

		// WHEN
		sm := GetStorageManager()

		// THEN
		assert.NotNil(t, sm)
		assert.NotNil(t, sm.BufferPool)
	})

	t.Run("初期化前に取得しようとすると panic", func(t *testing.T) {
		// GIVEN
		ResetStorageManager()

		// WHEN & THEN
		assert.Panics(t, func() {
			GetStorageManager()
		})
	})
}

func TestRegisterDmToBpm(t *testing.T) {
	t.Run("Disk を BufferPool に登録できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageManager()
		InitStorageManager()
		sm := GetStorageManager()

		fileId := page.FileId(1)
		tableName := "users"

		// WHEN
		err := sm.RegisterDmToBpm(fileId, tableName)

		// THEN
		assert.NoError(t, err)

		dm, err := sm.BufferPool.GetDisk(fileId)
		assert.NoError(t, err)
		assert.NotNil(t, dm)
	})

	t.Run("同じ FileId で 2 回登録しても問題ない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageManager()
		InitStorageManager()
		sm := GetStorageManager()

		fileId := page.FileId(1)
		tableName := "users"

		// WHEN
		err1 := sm.RegisterDmToBpm(fileId, tableName)
		err2 := sm.RegisterDmToBpm(fileId, tableName)

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)

		dm, err := sm.BufferPool.GetDisk(fileId)
		assert.NoError(t, err)
		assert.NotNil(t, dm)
	})
}

func TestInitCatalog(t *testing.T) {
	t.Run("カタログファイルが存在しない場合、新しいカタログが作成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageManager()

		// WHEN
		sm := InitStorageManager()

		// THEN
		assert.NotNil(t, sm)
		assert.NotNil(t, sm.Catalog)
		assert.Equal(t, uint64(0), sm.Catalog.NextTableId)
	})

	t.Run("カタログファイルが既に存在する場合、既存のカタログが開かれる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageManager()

		// 最初の初期化でカタログを作成
		engine1 := InitStorageManager()

		// テーブルIDを採番してディスクに保存
		_, err := engine1.Catalog.AllocateTableId(engine1.BufferPool)
		assert.NoError(t, err)
		_, err = engine1.Catalog.AllocateTableId(engine1.BufferPool)
		assert.NoError(t, err)

		// ダーティーページをディスクにフラッシュ
		err = engine1.BufferPool.FlushPage()
		assert.NoError(t, err)

		// StorageManager をリセット
		ResetStorageManager()

		// WHEN: 同じディレクトリで再初期化
		engine2 := InitStorageManager()

		// THEN: NextTableId が保存された値 (2) になっている
		assert.NotNil(t, engine2.Catalog)
		assert.Equal(t, uint64(2), engine2.Catalog.NextTableId)
	})

	t.Run("カタログの Disk が BufferPool に登録される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageManager()

		// WHEN
		sm := InitStorageManager()

		// THEN
		dm, err := sm.BufferPool.GetDisk(page.FileId(0))
		assert.NoError(t, err)
		assert.NotNil(t, dm)
	})

	t.Run("既存のテーブルがある場合、再初期化でテーブルの Disk が登録される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		ResetStorageManager()

		// テーブルを作成してカタログに登録
		sm1 := InitStorageManager()
		bp := sm1.BufferPool

		tableFileId := bp.AllocateFileId()
		err := sm1.RegisterDmToBpm(tableFileId, "users")
		assert.NoError(t, err)

		metaPageId, err := bp.AllocatePageId(tableFileId)
		assert.NoError(t, err)
		tbl := access.NewTableAccessMethod("users", metaPageId, 1, nil)
		err = tbl.Create(bp)
		assert.NoError(t, err)

		tblId, err := sm1.Catalog.AllocateTableId(bp)
		assert.NoError(t, err)

		cols := []*catalog.ColumnMetadata{
			catalog.NewColumnMetadata(tblId, "id", 0, catalog.ColumnTypeString),
		}
		tblMeta := catalog.NewTableMetadata(tblId, "users", 1, 1, cols, nil, metaPageId)
		err = sm1.Catalog.Insert(bp, tblMeta)
		assert.NoError(t, err)

		err = bp.FlushPage()
		assert.NoError(t, err)

		ResetStorageManager()

		// WHEN: 同じディレクトリで再初期化
		sm2 := InitStorageManager()

		// THEN: テーブルの Disk が登録されている
		dm, err := sm2.BufferPool.GetDisk(tableFileId)
		assert.NoError(t, err)
		assert.NotNil(t, dm)

		// カタログからテーブル情報も取得できる
		tableMeta, err := sm2.Catalog.GetTableMetadataByName("users")
		assert.NoError(t, err)
		assert.Equal(t, "users", tableMeta.Name)
	})

	t.Run("カタログファイルが空の場合、再作成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageManager()

		// 空のカタログファイルを作成
		catalogPath := filepath.Join(tmpdir, "minesql.db")
		err := os.WriteFile(catalogPath, []byte{}, 0600)
		assert.NoError(t, err)

		// WHEN
		sm := InitStorageManager()

		// THEN: 新しいカタログが作成され、NextTableId は 0
		assert.NotNil(t, sm)
		assert.NotNil(t, sm.Catalog)
		assert.Equal(t, uint64(0), sm.Catalog.NextTableId)
	})

	t.Run("データディレクトリが存在しない場合、自動作成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		nestedDir := filepath.Join(tmpdir, "nested", "data")
		t.Setenv("MINESQL_DATA_DIR", nestedDir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageManager()

		// WHEN
		sm := InitStorageManager()

		// THEN: ディレクトリが作成され、初期化が完了している
		assert.NotNil(t, sm)
		_, err := os.Stat(nestedDir)
		assert.NoError(t, err)
	})
}
