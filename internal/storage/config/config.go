package config

import (
	"os"
	"strconv"
)

// GetDataDirectory はデータディレクトリのパスを取得する
//
// 環境変数 MINESQL_DATA_DIR が設定されていればその値を、なければデフォルト値を返す
func GetDataDirectory() string {
	return getEnv("MINESQL_DATA_DIR", "./data")
}

// GetBufferPoolSize はバッファプールのサイズを取得する
//
// 環境変数 MINESQL_BUFFER_SIZE が設定されていればその値を、なければデフォルト値を返す
func GetBufferPoolSize() int {
	return getEnvInt("MINESQL_BUFFER_SIZE", 100)
}

// GetLockWaitTimeout はロック取得のタイムアウト値 (ミリ秒) を取得する
//
// 環境変数 MINESQL_LOCK_WAIT_TIMEOUT が設定されていればその値を、なければデフォルト値を返す
func GetLockWaitTimeout() int {
	return getEnvInt("MINESQL_LOCK_WAIT_TIMEOUT", 30000)
}

// GetRedoLogMaxSize は REDO ログの最大サイズ (バイト) を取得する
//
// 環境変数 MINESQL_REDO_LOG_MAX_SIZE が設定されていればその値を、なければデフォルト値を返す
func GetRedoLogMaxSize() int {
	return getEnvInt("MINESQL_REDO_LOG_MAX_SIZE", 1048576) // 1MB
}

// GetMaxDirtyPagesPct はダーティーページ率の上限 (%) を取得する
//
// 環境変数 MINESQL_MAX_DIRTY_PAGES_PCT が設定されていればその値を、なければデフォルト値を返す
func GetMaxDirtyPagesPct() int {
	return getEnvInt("MINESQL_MAX_DIRTY_PAGES_PCT", 90)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}
