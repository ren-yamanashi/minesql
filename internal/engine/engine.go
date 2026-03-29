package engine

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"minesql/internal/catalog"
	"minesql/internal/config"
	"minesql/internal/storage"
)

var (
	eng  *Engine
	once sync.Once
)

// Engine はストレージ層のリソースの管理を行う
type Engine struct {
	BufferPool    *storage.BufferPool
	Catalog       *catalog.Catalog
	baseDirectory string
}

// グローバルな Engine を初期化する
func Init() *Engine {
	once.Do(func() {
		e, err := newEngine()
		if err != nil {
			panic(fmt.Sprintf("failed to initialize engine: %v", err))
		}
		eng = e
	})
	return eng
}

// Reset はグローバルな Engine の状態をリセットする (主にテストで使用)
func Reset() {
	eng = nil
	once = sync.Once{}
}

// Get はグローバルな Engine を取得する
func Get() *Engine {
	if eng == nil {
		panic("engine not initialized. call engine.Init() first")
	}
	return eng
}

// RegisterDmToBp は BufferPool に Disk を登録する
func (e *Engine) RegisterDmToBp(fileId storage.FileId, tableName string) error {
	path := filepath.Join(e.baseDirectory, fmt.Sprintf("%s.db", tableName))
	dm, err := storage.NewDisk(fileId, path)
	if err != nil {
		return err
	}
	e.BufferPool.RegisterDisk(fileId, dm)
	return nil
}

// newEngine は Engine を初期化する
func newEngine() (*Engine, error) {
	dataDir := config.GetDataDirectory()
	err := os.MkdirAll(dataDir, 0750)
	if err != nil {
		return nil, err
	}

	bp := storage.NewBufferPool(config.GetBufferPoolSize())
	catalog, err := initCatalog(dataDir, bp)
	if err != nil {
		return nil, err
	}

	return &Engine{
		BufferPool:    bp,
		Catalog:       catalog,
		baseDirectory: dataDir,
	}, nil
}

// initCatalog はカタログを初期化する
func initCatalog(baseDir string, bp *storage.BufferPool) (*catalog.Catalog, error) {
	fileId := storage.FileId(0)
	path := filepath.Join(baseDir, "minesql.db")

	// カタログファイルが存在するかチェック (Disk 作成前に確認)
	_, err := os.Stat(path)
	catalogExists := !os.IsNotExist(err)

	dm, err := storage.NewDisk(fileId, path)
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

// registerTableDisks はカタログに含まれるテーブルの Disk を登録する
func registerTableDisks(cat *catalog.Catalog, baseDir string, bp *storage.BufferPool) error {
	tables := cat.GetAllTables()
	for _, tableMeta := range tables {
		fileId := tableMeta.DataMetaPageId.FileId
		tableName := tableMeta.Name
		path := filepath.Join(baseDir, fmt.Sprintf("%s.db", tableName))

		// Disk を作成して登録
		dm, err := storage.NewDisk(fileId, path)
		if err != nil {
			return err
		}
		bp.RegisterDisk(fileId, dm)
	}
	return nil
}
