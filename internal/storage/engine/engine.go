package engine

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"minesql/internal/config"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/catalog"
	"minesql/internal/storage/file"
	"minesql/internal/storage/page"
	"minesql/internal/storage/statistics"
	"minesql/internal/storage/transaction"
)

// TrxId はトランザクション ID の型 (storage/transaction.TrxId のエイリアス)
type TrxId = transaction.TrxId

// UndoLog は Undo ログの型 (storage/transaction.UndoLog のエイリアス)
type UndoLog = transaction.UndoLog

// TableMetadata はテーブルメタデータの型 (storage/catalog.TableMetadata のエイリアス)
type TableMetadata = catalog.TableMetadata

// IndexMetadata はインデックスメタデータの型 (storage/catalog.IndexMetadata のエイリアス)
type IndexMetadata = catalog.IndexMetadata

// ColumnType はカラムの型 (storage/catalog.ColumnType のエイリアス)
type ColumnType = catalog.ColumnType

// ColumnTypeString は文字列型を表す ColumnType 定数
const ColumnTypeString = catalog.ColumnTypeString

// TableStatistics はテーブル統計情報の型 (storage/statistics.TableStatistics のエイリアス)
type TableStatistics = statistics.TableStatistics

// IndexStatistics はインデックス統計情報の型 (storage/statistics.IndexStatistics のエイリアス)
type IndexStatistics = statistics.IndexStatistics

// ColumnStatistics はカラム統計情報の型 (storage/statistics.ColumnStatistics のエイリアス)
type ColumnStatistics = statistics.ColumnStatistics

var (
	eng  *Engine
	once sync.Once
)

// Engine はストレージ層のリソースの管理を行う (MySQL の handler に相当)
type Engine struct {
	BufferPool    *buffer.BufferPool
	Catalog       *catalog.Catalog
	undoLog       *transaction.UndoLog
	trxManager    *transaction.Manager
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

// Shutdown はダーティーページをディスクに書き出し、すべての Disk を同期する
func (e *Engine) Shutdown() error {
	if err := e.BufferPool.FlushPage(); err != nil {
		return err
	}
	return e.syncAllDisks()
}

// syncAllDisks はカタログと全テーブルの Disk を同期する
func (e *Engine) syncAllDisks() error {
	// カタログの Disk を同期
	catalogDisk, err := e.BufferPool.GetDisk(page.FileId(0))
	if err != nil {
		return err
	}
	if err := catalogDisk.Sync(); err != nil {
		return err
	}

	// 各テーブルの Disk を同期
	for _, table := range e.Catalog.GetAllTables() {
		dm, err := e.BufferPool.GetDisk(table.FileId)
		if err != nil {
			return err
		}
		if err := dm.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// RegisterDmToBp は BufferPool に Disk を登録する
func (e *Engine) RegisterDmToBp(fileId page.FileId, tableName string) error {
	path := filepath.Join(e.baseDirectory, fmt.Sprintf("%s.db", tableName))
	dm, err := file.NewDisk(fileId, path)
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

	bp := buffer.NewBufferPool(config.GetBufferPoolSize())
	catalog, err := initCatalog(dataDir, bp)
	if err != nil {
		return nil, err
	}

	undoLog := transaction.NewUndoLog()

	return &Engine{
		BufferPool:    bp,
		Catalog:       catalog,
		undoLog:       undoLog,
		trxManager:    transaction.NewManager(undoLog),
		baseDirectory: dataDir,
	}, nil
}

// initCatalog はカタログを初期化する
func initCatalog(baseDir string, bp *buffer.BufferPool) (*catalog.Catalog, error) {
	fileId := page.FileId(0)
	path := filepath.Join(baseDir, "minesql.db")

	// カタログファイルが存在するかチェック (Disk 作成前に確認)
	_, err := os.Stat(path)
	catalogExists := !os.IsNotExist(err)

	dm, err := file.NewDisk(fileId, path)
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
func registerTableDisks(cat *catalog.Catalog, baseDir string, bp *buffer.BufferPool) error {
	tables := cat.GetAllTables()
	for _, tableMeta := range tables {
		fileId := tableMeta.DataMetaPageId.FileId
		tableName := tableMeta.Name
		path := filepath.Join(baseDir, fmt.Sprintf("%s.db", tableName))

		// Disk を作成して登録
		dm, err := file.NewDisk(fileId, path)
		if err != nil {
			return err
		}
		bp.RegisterDisk(fileId, dm)
	}
	return nil
}
