package config

import (
	"os"
	"strconv"
)

// データディレクトリのパスを取得
// 環境変数 MINESQL_DATA_DIR が設定されていればその値を、なければデフォルト値を返す
func GetDataDirectory() string {
	return getEnv("MINESQL_DATA_DIR", "./data")
}

// バッファプールのサイズを取得
// 環境変数 MINESQL_BUFFER_SIZE が設定されていればその値を、なければデフォルト値を返す
func GetBufferPoolSize() int {
	return getEnvInt("MINESQL_BUFFER_SIZE", 100)
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
