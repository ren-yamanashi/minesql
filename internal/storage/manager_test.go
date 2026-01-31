package storage

import (
	"minesql/internal/storage/page"
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
		assert.NotNil(t, sm.BufferPoolManager)
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
	t.Run("DiskManager を BufferPoolManager に登録できる", func(t *testing.T) {
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

		dm, err := sm.BufferPoolManager.GetDiskManager(fileId)
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
		_, err := engine1.Catalog.AllocateTableId(engine1.BufferPoolManager)
		assert.NoError(t, err)
		_, err = engine1.Catalog.AllocateTableId(engine1.BufferPoolManager)
		assert.NoError(t, err)

		// ダーティーページをディスクにフラッシュ
		err = engine1.BufferPoolManager.FlushPage()
		assert.NoError(t, err)

		// StorageManager をリセット
		ResetStorageManager()

		// WHEN: 同じディレクトリで再初期化
		engine2 := InitStorageManager()

		// THEN: NextTableId が保存された値 (2) になっている
		assert.NotNil(t, engine2.Catalog)
		assert.Equal(t, uint64(2), engine2.Catalog.NextTableId)
	})

	t.Run("カタログの DiskManager が BufferPoolManager に登録される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageManager()

		// WHEN
		sm := InitStorageManager()

		// THEN
		dm, err := sm.BufferPoolManager.GetDiskManager(page.FileId(0))
		assert.NoError(t, err)
		assert.NotNil(t, dm)
	})
}
