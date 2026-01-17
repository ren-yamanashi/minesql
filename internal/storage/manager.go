package storage

import (
	"fmt"
	"minesql/internal/config"
	"minesql/internal/storage/access/catalog"
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
		mng, err := newStorageManager()
		if err != nil {
			panic(fmt.Sprintf("failed to initialize storage manager: %v", err))
		}
		manager = mng
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
	catalog           *catalog.Catalog
	baseDirectory     string
}

func newStorageManager() (*StorageManager, error) {
	dataDir := config.GetDataDirectory()
	os.MkdirAll(dataDir, 0755)

	bpm := bufferpool.NewBufferPoolManager(config.GetBufferPoolSize())
	catalog, err := initCatalog(dataDir, bpm)
	if err != nil {
		return nil, err
	}
	return &StorageManager{
		bufferPoolManager: bpm,
		tables:            make(map[string]*table.Table),
		baseDirectory:     dataDir,
		catalog:           catalog,
	}, nil
}

// BufferPoolManager を取得する
func (se *StorageManager) GetBufferPoolManager() *bufferpool.BufferPoolManager {
	return se.bufferPoolManager
}

// カタログを取得する
func (se *StorageManager) GetCatalog() *catalog.Catalog {
	return se.catalog
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

// カタログを初期化する
func initCatalog(baseDir string, bpm *bufferpool.BufferPoolManager) (*catalog.Catalog, error) {
	fileId := page.FileId(0)
	path := filepath.Join(baseDir, "minesql.db")

	// カタログファイルが存在するかチェック (DiskManager 作成前に確認)
	_, err := os.Stat(path)
	catalogExists := !os.IsNotExist(err)

	dm, err := disk.NewDiskManager(fileId, path)
	if err != nil {
		return nil, err
	}
	bpm.RegisterDiskManager(fileId, dm)

	var cat *catalog.Catalog
	if catalogExists {
		// 既存のカタログを開く
		cat, err = catalog.NewCatalog(bpm)
		if err != nil {
			return nil, err
		}
	} else {
		// 新しいカタログを作成
		cat, err = catalog.CreateCatalog(bpm)
		if err != nil {
			return nil, err
		}
	}

	return cat, nil
}
