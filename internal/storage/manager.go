package storage

import (
	"fmt"
	"minesql/internal/config"
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
	"minesql/internal/storage/page"
	"os"
	"path/filepath"
	"sync"
)

var (
	manager *StorageManager
	once    sync.Once
)

// グローバルな StorageManager を初期化する
func InitStorageManager() *StorageManager {
	once.Do(func() {
		manager = newStorageManager()
	})
	return manager
}

func GetStorageManager() *StorageManager {
	if manager == nil {
		panic("storage manager not initialized. call InitStorageManager() first")
	}
	return manager
}

// ストレージエンジン層のリソースの管理を行う
type StorageManager struct {
	bufferPoolManager *bufferpool.BufferPoolManager
	tables            map[string]*table.Table
	baseDirectory     string
}

func newStorageManager() *StorageManager {
	dataDir := config.GetDataDirectory()
	os.MkdirAll(dataDir, 0755)

	return &StorageManager{
		bufferPoolManager: bufferpool.NewBufferPoolManager(config.GetBufferPoolSize()),
		tables:            make(map[string]*table.Table),
		baseDirectory:     dataDir,
	}
}

// BufferPoolManager を取得する
func (se *StorageManager) GetBufferPoolManager() *bufferpool.BufferPoolManager {
	return se.bufferPoolManager
}

// テーブル名から Table を取得する
func (se *StorageManager) GetTable(tableName string) (*table.Table, error) {
	tbl, ok := se.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	return tbl, nil
}

// BufferPoolManager に DiskManager を登録する
func (se *StorageManager) RegisterDmToBpm(fileId page.FileId, tableName string) error {
	path := filepath.Join(se.baseDirectory, fmt.Sprintf("%s.db", tableName))
	dm, err := disk.NewDiskManager(fileId, path)
	if err != nil {
		return err
	}
	se.bufferPoolManager.RegisterDiskManager(fileId, dm)
	return nil
}

// テーブルを登録する
func (se *StorageManager) RegisterTable(tbl *table.Table) {
	se.tables[tbl.Name] = tbl
}
