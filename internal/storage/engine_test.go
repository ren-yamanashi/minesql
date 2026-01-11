package storage

import (
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
