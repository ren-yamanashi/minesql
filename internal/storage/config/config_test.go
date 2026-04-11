package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetDataDirectory(t *testing.T) {
	t.Run("環境変数が設定されていない場合、デフォルト値を返す", func(t *testing.T) {
		// GIVEN
		t.Setenv("MINESQL_DATA_DIR", "")

		// WHEN
		result := GetDataDirectory()

		// THEN
		assert.Equal(t, "./data", result)
	})

	t.Run("環境変数が設定されている場合、その値を返す", func(t *testing.T) {
		// GIVEN
		t.Setenv("MINESQL_DATA_DIR", "/tmp/minesql")

		// WHEN
		result := GetDataDirectory()

		// THEN
		assert.Equal(t, "/tmp/minesql", result)
	})
}

func TestGetBufferPoolSize(t *testing.T) {
	t.Run("環境変数が設定されていない場合、デフォルト値を返す", func(t *testing.T) {
		// GIVEN
		t.Setenv("MINESQL_BUFFER_SIZE", "")

		// WHEN
		result := GetBufferPoolSize()

		// THEN
		assert.Equal(t, 100, result)
	})

	t.Run("環境変数が設定されている場合、その値を返す", func(t *testing.T) {
		// GIVEN
		t.Setenv("MINESQL_BUFFER_SIZE", "256")

		// WHEN
		result := GetBufferPoolSize()

		// THEN
		assert.Equal(t, 256, result)
	})

	t.Run("環境変数が数値でない場合、デフォルト値を返す", func(t *testing.T) {
		// GIVEN
		t.Setenv("MINESQL_BUFFER_SIZE", "abc")

		// WHEN
		result := GetBufferPoolSize()

		// THEN
		assert.Equal(t, 100, result)
	})
}

func TestGetLockWaitTimeout(t *testing.T) {
	t.Run("環境変数が設定されていない場合、デフォルト値を返す", func(t *testing.T) {
		// GIVEN
		t.Setenv("MINESQL_LOCK_WAIT_TIMEOUT", "")

		// WHEN
		result := GetLockWaitTimeout()

		// THEN
		assert.Equal(t, 30000, result)
	})

	t.Run("環境変数が設定されている場合、その値を返す", func(t *testing.T) {
		// GIVEN
		t.Setenv("MINESQL_LOCK_WAIT_TIMEOUT", "5000")

		// WHEN
		result := GetLockWaitTimeout()

		// THEN
		assert.Equal(t, 5000, result)
	})

	t.Run("環境変数が数値でない場合、デフォルト値を返す", func(t *testing.T) {
		// GIVEN
		t.Setenv("MINESQL_LOCK_WAIT_TIMEOUT", "invalid")

		// WHEN
		result := GetLockWaitTimeout()

		// THEN
		assert.Equal(t, 30000, result)
	})
}

func TestGetRedoLogMaxSize(t *testing.T) {
	t.Run("環境変数が設定されていない場合、デフォルト値を返す", func(t *testing.T) {
		// GIVEN
		t.Setenv("MINESQL_REDO_LOG_MAX_SIZE", "")

		// WHEN
		result := GetRedoLogMaxSize()

		// THEN
		assert.Equal(t, 1048576, result)
	})

	t.Run("環境変数が設定されている場合、その値を返す", func(t *testing.T) {
		// GIVEN
		t.Setenv("MINESQL_REDO_LOG_MAX_SIZE", "2097152")

		// WHEN
		result := GetRedoLogMaxSize()

		// THEN
		assert.Equal(t, 2097152, result)
	})

	t.Run("環境変数が数値でない場合、デフォルト値を返す", func(t *testing.T) {
		// GIVEN
		t.Setenv("MINESQL_REDO_LOG_MAX_SIZE", "abc")

		// WHEN
		result := GetRedoLogMaxSize()

		// THEN
		assert.Equal(t, 1048576, result)
	})
}

func TestGetMaxDirtyPagesPct(t *testing.T) {
	t.Run("環境変数が設定されていない場合、デフォルト値を返す", func(t *testing.T) {
		// GIVEN
		t.Setenv("MINESQL_MAX_DIRTY_PAGES_PCT", "")

		// WHEN
		result := GetMaxDirtyPagesPct()

		// THEN
		assert.Equal(t, 90, result)
	})

	t.Run("環境変数が設定されている場合、その値を返す", func(t *testing.T) {
		// GIVEN
		t.Setenv("MINESQL_MAX_DIRTY_PAGES_PCT", "75")

		// WHEN
		result := GetMaxDirtyPagesPct()

		// THEN
		assert.Equal(t, 75, result)
	})

	t.Run("環境変数が数値でない場合、デフォルト値を返す", func(t *testing.T) {
		// GIVEN
		t.Setenv("MINESQL_MAX_DIRTY_PAGES_PCT", "invalid")

		// WHEN
		result := GetMaxDirtyPagesPct()

		// THEN
		assert.Equal(t, 90, result)
	})
}
