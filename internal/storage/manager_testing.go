package storage

import "sync"

// NOTE: テスト用
func ResetStorageManager() {
	manager = nil
	once = sync.Once{}
}
