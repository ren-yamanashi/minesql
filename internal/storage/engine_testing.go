package storage

import "sync"

// NOTE: テスト用
func ResetStorageEngine() {
	globalEngine = nil
	once = sync.Once{}
}
