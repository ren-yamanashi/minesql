package storage

import (
	"errors"
	"fmt"
	"io"
	"minesql/internal/config"
	"minesql/internal/storage/access/catalog"
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
	BufferPoolManager *bufferpool.BufferPoolManager
	Catalog           *catalog.Catalog
	baseDirectory     string
}

func newStorageManager() (*StorageManager, error) {
	dataDir := config.GetDataDirectory()
	err := os.MkdirAll(dataDir, 0750)
	if err != nil {
		return nil, err
	}

	bpm := bufferpool.NewBufferPoolManager(config.GetBufferPoolSize())
	catalog, err := initCatalog(dataDir, bpm)
	if err != nil {
		return nil, err
	}
	return &StorageManager{
		BufferPoolManager: bpm,
		Catalog:           catalog,
		baseDirectory:     dataDir,
	}, nil
}

// BufferPoolManager に DiskManager を登録する
func (sm *StorageManager) RegisterDmToBpm(fileId page.FileId, tableName string) error {
	path := filepath.Join(sm.baseDirectory, fmt.Sprintf("%s.db", tableName))
	dm, err := disk.NewDiskManager(fileId, path)
	if err != nil {
		return err
	}
	sm.BufferPoolManager.RegisterDiskManager(fileId, dm)
	return nil
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
		// カタログファイルが空の場合は古いファイルを削除して再度実行
		if errors.Is(err, io.EOF) {
			err := os.Remove(path)
			if err != nil {
				return nil, err
			}
			return initCatalog(baseDir, bpm)
		}
		// その他のエラーの場合はそのまま返す
		if err != nil {
			return nil, err
		}

		// 既存のテーブルの DiskManager を登録
		if err := registerTableDiskManagers(cat, baseDir, bpm); err != nil {
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

// カタログに含まれるテーブルの DiskManager を登録する
func registerTableDiskManagers(cat *catalog.Catalog, baseDir string, bpm *bufferpool.BufferPoolManager) error {
	tables := cat.GetAllTables()
	for _, tableMeta := range tables {
		fileId := tableMeta.DataMetaPageId.FileId
		tableName := tableMeta.Name
		path := filepath.Join(baseDir, fmt.Sprintf("%s.db", tableName))

		// DiskManager を作成して登録
		dm, err := disk.NewDiskManager(fileId, path)
		if err != nil {
			return err
		}
		bpm.RegisterDiskManager(fileId, dm)
	}
	return nil
}
