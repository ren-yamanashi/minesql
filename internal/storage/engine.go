package storage

import (
	"fmt"
	"minesql/internal/config"
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
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

func (se *StorageEngine) GetTableHandle(tableName string) (*TableHandler, error) {
	tbl, ok := se.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	return NewTableHandle(tbl, se.bufferPoolManager), nil
}

func (se *StorageEngine) CreateTable(name string, primaryKeyCount int, uniqueIndexes []*table.UniqueIndex) (*table.Table, error) {
	// FileId を割り当て
	fileId := se.bufferPoolManager.AllocateFileId()

	// ディスクファイルを作成
	filePath := filepath.Join(se.baseDirectory, fmt.Sprintf("%s.db", name))
	dm, err := disk.NewDiskManager(fileId, filePath)
	if err != nil {
		return nil, err
	}

	// DiskManager を登録
	se.bufferPoolManager.RegisterDiskManager(fileId, dm)

	// テーブルの metaPageId を割り当て
	metaPageId, err := se.bufferPoolManager.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}

	// 各 UniqueIndex の metaPageId を割り当て
	for _, ui := range uniqueIndexes {
		indexMetaPageId, err := se.bufferPoolManager.AllocatePageId(fileId)
		if err != nil {
			return nil, err
		}
		ui.MetaPageId = indexMetaPageId
	}

	// テーブルを作成
	tbl := table.NewTable(name, metaPageId, primaryKeyCount, uniqueIndexes)
	err = tbl.Create(se.bufferPoolManager)
	if err != nil {
		return nil, err
	}

	// テーブルマップに登録
	if _, ok := se.tables[name]; ok {
		return nil, fmt.Errorf("table %s already exists", name)
	}
	se.tables[name] = &tbl

	return &tbl, nil
}

func (se *StorageEngine) FlushAll() error {
	return se.bufferPoolManager.FlushPage()
}
