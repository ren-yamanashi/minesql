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
	globalEngine *StorageEngine
	once         sync.Once
)

func InitStorageEngine() *StorageEngine {
	once.Do(func() {
		globalEngine = newStorageEngine()
	})
	return globalEngine
}

func GetStorageEngine() *StorageEngine {
	if globalEngine == nil {
		panic("storage engine not initialized. call InitStorageEngine() first")
	}
	return globalEngine
}

type StorageEngine struct {
	bufferPoolManager *bufferpool.BufferPoolManager
	tables            map[string]*table.Table
	baseDirectory     string
}

func newStorageEngine() *StorageEngine {
	dataDir := config.GetDataDirectory()
	os.MkdirAll(dataDir, 0755)

	return &StorageEngine{
		bufferPoolManager: bufferpool.NewBufferPoolManager(config.GetBufferPoolSize(), dataDir),
		tables:            make(map[string]*table.Table),
		baseDirectory:     dataDir,
	}
}

func (se *StorageEngine) GetBufferPoolManager() *bufferpool.BufferPoolManager {
	return se.bufferPoolManager
}

func (se *StorageEngine) GetTable(tableName string) (*table.Table, error) {
	tbl, ok := se.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	return tbl, nil
}

// BufferPoolManager に DiskManager を登録する
func (se *StorageEngine) RegisterDmToBpm(fileId page.FileId, tableName string) error {
	path := filepath.Join(se.baseDirectory, fmt.Sprintf("%s.db", tableName))
	dm, err := disk.NewDiskManager(fileId, path)
	if err != nil {
		return err
	}
	se.bufferPoolManager.RegisterDiskManager(fileId, dm)
	return nil
}

func (se *StorageEngine) RegisterTable(tbl *table.Table) {
	se.tables[tbl.Name] = tbl
}
