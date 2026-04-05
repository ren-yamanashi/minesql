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
