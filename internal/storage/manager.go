package storage

import (
	"errors"
	"fmt"
	"io"
	"minesql/internal/config"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/catalog"
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

// ResetStorageManager はグローバルな StorageManager の状態をリセットする (主にテストで使用)
func ResetStorageManager() {
	manager = nil
	once = sync.Once{}
}

func GetStorageManager() *StorageManager {
	if manager == nil {
		panic("storage manager not initialized. call InitStorageManager() first")
	}
	return manager
}

// ストレージエンジン層のリソースの管理を行う
type StorageManager struct {
	BufferPool    *bufferpool.BufferPool
	Catalog       *catalog.Catalog
	baseDirectory string
}

func newStorageManager() (*StorageManager, error) {
	dataDir := config.GetDataDirectory()
	err := os.MkdirAll(dataDir, 0750)
	if err != nil {
		return nil, err
	}

	bp := bufferpool.NewBufferPool(config.GetBufferPoolSize())
	catalog, err := initCatalog(dataDir, bp)
	if err != nil {
		return nil, err
	}
	return &StorageManager{
		BufferPool:    bp,
		Catalog:       catalog,
		baseDirectory: dataDir,
	}, nil
}

// BufferPool に Disk を登録する
func (sm *StorageManager) RegisterDmToBpm(fileId page.FileId, tableName string) error {
	path := filepath.Join(sm.baseDirectory, fmt.Sprintf("%s.db", tableName))
	dm, err := disk.NewDisk(fileId, path)
	if err != nil {
		return err
	}
	sm.BufferPool.RegisterDisk(fileId, dm)
	return nil
}

// カタログを初期化する
func initCatalog(baseDir string, bp *bufferpool.BufferPool) (*catalog.Catalog, error) {
	fileId := page.FileId(0)
	path := filepath.Join(baseDir, "minesql.db")

	// カタログファイルが存在するかチェック (Disk 作成前に確認)
	_, err := os.Stat(path)
	catalogExists := !os.IsNotExist(err)

	dm, err := disk.NewDisk(fileId, path)
	if err != nil {
		return nil, err
	}
	bp.RegisterDisk(fileId, dm)

	var cat *catalog.Catalog
	if catalogExists {
		// 既存のカタログを開く
		cat, err = catalog.NewCatalog(bp)
		// カタログファイルが空の場合は古いファイルを削除して再度実行
		if errors.Is(err, io.EOF) {
			err := os.Remove(path)
			if err != nil {
				return nil, err
			}
			return initCatalog(baseDir, bp)
		}
		// その他のエラーの場合はそのまま返す
		if err != nil {
			return nil, err
		}

		// 既存のテーブルの Disk を登録
		if err := registerTableDisks(cat, baseDir, bp); err != nil {
			return nil, err
		}
	} else {
		// 新しいカタログを作成
		cat, err = catalog.CreateCatalog(bp)
		if err != nil {
			return nil, err
		}
	}

	return cat, nil
}

// カタログに含まれるテーブルの Disk を登録する
func registerTableDisks(cat *catalog.Catalog, baseDir string, bp *bufferpool.BufferPool) error {
	tables := cat.GetAllTables()
	for _, tableMeta := range tables {
		fileId := tableMeta.DataMetaPageId.FileId
		tableName := tableMeta.Name
		path := filepath.Join(baseDir, fmt.Sprintf("%s.db", tableName))

		// Disk を作成して登録
		dm, err := disk.NewDisk(fileId, path)
		if err != nil {
			return err
		}
		bp.RegisterDisk(fileId, dm)
	}
	return nil
}
