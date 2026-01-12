package storage

import (
	"minesql/internal/storage/access/table"
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
		engine := GetStorageManager()

		// THEN
		assert.NotNil(t, engine)
		assert.NotNil(t, engine.bufferPoolManager)
		assert.NotNil(t, engine.tables)
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

func TestGetBufferPoolManager(t *testing.T) {
	t.Run("BufferPoolManager を取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageManager()
		InitStorageManager()
		engine := GetStorageManager()

		// WHEN
		bpm := engine.GetBufferPoolManager()

		// THEN
		assert.NotNil(t, bpm)
	})
}

func TestGetTable(t *testing.T) {
	t.Run("登録されたテーブルを取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageManager()
		InitStorageManager()
		engine := GetStorageManager()

		tbl := &table.Table{Name: "users"}
		engine.RegisterTable(tbl)

		// WHEN
		retrievedTbl, err := engine.GetTable("users")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, tbl, retrievedTbl)
	})

	t.Run("存在しないテーブルを取得しようとするとエラー", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageManager()
		InitStorageManager()
		engine := GetStorageManager()

		// WHEN
		retrievedTbl, err := engine.GetTable("non_existent_table")

		// THEN
		assert.Nil(t, retrievedTbl)
		assert.Error(t, err)
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
		engine := GetStorageManager()

		fileId := page.FileId(1)
		tableName := "users"

		// WHEN
		err := engine.RegisterDmToBpm(fileId, tableName)

		// THEN
		assert.NoError(t, err)

		bpm := engine.GetBufferPoolManager()
		dm, err := bpm.GetDiskManager(fileId)
		assert.NoError(t, err)
		assert.NotNil(t, dm)
	})
}

func TestRegisterTable(t *testing.T) {
	t.Run("テーブルを登録できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageManager()
		InitStorageManager()
		engine := GetStorageManager()

		tbl := &table.Table{Name: "users"}

		// WHEN
		engine.RegisterTable(tbl)

		// THEN
		retrievedTbl, err := engine.GetTable("users")
		assert.NoError(t, err)
		assert.Equal(t, tbl, retrievedTbl)
	})
}
