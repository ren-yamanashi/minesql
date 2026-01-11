package storage

import (
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/disk"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitGlobalEngine(t *testing.T) {
	t.Run("グローバル StorageEngine を初期化できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()

		// WHEN
		InitStorageEngine()

		// THEN
		assert.NotNil(t, globalEngine)
	})

	t.Run("複数回初期化しても同じインスタンスが返される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()

		// WHEN
		engine1 := InitStorageEngine()
		engine2 := InitStorageEngine()

		// THEN
		assert.Same(t, engine1, engine2)
	})
}

func TestGetGlobalEngine(t *testing.T) {
	t.Run("初期化後にグローバル StorageEngine を取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()
		InitStorageEngine()

		// WHEN
		engine := GetStorageEngine()

		// THEN
		assert.NotNil(t, engine)
		assert.NotNil(t, engine.bufferPoolManager)
		assert.NotNil(t, engine.tables)
	})

	t.Run("初期化前に取得しようとすると panic", func(t *testing.T) {
		// GIVEN
		ResetStorageEngine()

		// WHEN & THEN
		assert.Panics(t, func() {
			GetStorageEngine()
		})
	})
}

func TestResetGlobalEngine(t *testing.T) {
	t.Run("グローバル StorageEngine をリセットできる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()
		InitStorageEngine()
		engine1 := GetStorageEngine()

		// WHEN
		ResetStorageEngine()
		InitStorageEngine()
		engine2 := GetStorageEngine()

		// THEN
		assert.NotSame(t, engine1, engine2)
	})
}

func TestCreateTable(t *testing.T) {
	t.Run("テーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()
		InitStorageEngine()
		engine := GetStorageEngine()

		// WHEN
		tbl, err := engine.CreateTable("users", 1, []*table.UniqueIndex{})

		// THEN
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
		ResetStorageEngine()
		InitStorageEngine()
		engine := GetStorageEngine()
		uniqueIndex := table.NewUniqueIndex("email", 1)

		// WHEN
		tbl, err := engine.CreateTable("users", 1, []*table.UniqueIndex{uniqueIndex})

		// THEN
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
		ResetStorageEngine()
		InitStorageEngine()
		engine := GetStorageEngine()

		// WHEN
		tbl, err := engine.CreateTable("users", 1, []*table.UniqueIndex{})

		// THEN
		assert.NoError(t, err)
		// FileId が採番されていることを確認
		assert.NotEqual(t, disk.FileId(0), tbl.MetaPageId.FileId)
		// ディスクマネージャが登録されていることを確認
		bpm := engine.GetBufferPoolManager()
		dm, dmErr := bpm.GetDiskManager(tbl.MetaPageId.FileId)
		assert.NoError(t, dmErr)
		assert.NotNil(t, dm)
	})
}

func TestGetTableHandle(t *testing.T) {
	t.Run("TableHandle を取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()
		InitStorageEngine()
		engine := GetStorageEngine()
		engine.CreateTable("users", 1, []*table.UniqueIndex{})

		// WHEN
		handle, err := engine.GetTableHandle("users")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, handle)
		assert.NotNil(t, handle.bufferPoolManager)
		assert.Equal(t, "users", handle.table.Name)
		assert.Equal(t, handle.table.MetaPageId, engine.tables["users"].MetaPageId)
	})

	t.Run("存在しないテーブルの TableHandle を取得するとエラー", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()
		InitStorageEngine()
		engine := GetStorageEngine()

		// WHEN
		handle, err := engine.GetTableHandle("nonexistent")

		// THEN
		assert.Error(t, err)
		assert.Nil(t, handle)
	})
}

func TestFlushAll(t *testing.T) {
	t.Run("バッファプールの内容をディスクにフラッシュできる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()
		InitStorageEngine()
		engine := GetStorageEngine()
		tbl, _ := engine.CreateTable("users", 1, []*table.UniqueIndex{})
		bpm := engine.GetBufferPoolManager()
		tbl.Insert(bpm, [][]byte{[]byte("user1"), []byte("data1")})

		// WHEN
		err := engine.FlushAll()

		// THEN
		assert.NoError(t, err)
	})
}

func TestGetBufferPoolManager(t *testing.T) {
	t.Run("BufferPoolManager を取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()
		InitStorageEngine()
		engine := GetStorageEngine()

		// WHEN
		bpm := engine.GetBufferPoolManager()

		// THEN
		assert.NotNil(t, bpm)
	})
}
