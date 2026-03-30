package handler

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"minesql/internal/config"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/dictionary"
	"minesql/internal/storage/file"
	"minesql/internal/storage/page"
	"minesql/internal/storage/transaction"
)

// TrxId はトランザクション ID の型 (storage/transaction.TrxId のエイリアス)
type TrxId = transaction.TrxId

// UndoLog は Undo ログの型 (storage/transaction.UndoLog のエイリアス)
type UndoLog = transaction.UndoLog

// TableMetadata はテーブルメタデータの型 (storage/dictionary.TableMetadata のエイリアス)
type TableMetadata = dictionary.TableMetadata

// IndexMetadata はインデックスメタデータの型 (storage/dictionary.IndexMetadata のエイリアス)
type IndexMetadata = dictionary.IndexMetadata

// ColumnType はカラムの型 (storage/dictionary.ColumnType のエイリアス)
type ColumnType = dictionary.ColumnType

// ColumnTypeString は文字列型を表す ColumnType 定数
const ColumnTypeString = dictionary.ColumnTypeString

// TableStatistics はテーブル統計情報の型 (storage/dictionary.TableStatistics のエイリアス)
type TableStatistics = dictionary.TableStatistics

// IndexStatistics はインデックス統計情報の型 (storage/dictionary.IndexStatistics のエイリアス)
type IndexStatistics = dictionary.IndexStatistics

// ColumnStatistics はカラム統計情報の型 (storage/dictionary.ColumnStatistics のエイリアス)
type ColumnStatistics = dictionary.ColumnStatistics

var (
	hdl  *Handler
	once sync.Once
)

// Handler はストレージ層のリソースの管理を行う (MySQL の handler に相当)
type Handler struct {
	BufferPool    *buffer.BufferPool
	Catalog       *dictionary.Catalog
	undoLog       *transaction.UndoLog
	trxManager    *transaction.Manager
	baseDirectory string
}

// グローバルな Handler を初期化する
func Init() *Handler {
	once.Do(func() {
		h, err := newHandler()
		if err != nil {
			panic(fmt.Sprintf("failed to initialize handler: %v", err))
		}
		hdl = h
	})
	return hdl
}

// Reset はグローバルな Handler の状態をリセットする (主にテストで使用)
func Reset() {
	hdl = nil
	once = sync.Once{}
}

// Get はグローバルな Handler を取得する
func Get() *Handler {
	if hdl == nil {
		panic("handler not initialized. call handler.Init() first")
	}
	return hdl
}

// Shutdown はダーティーページをディスクに書き出し、すべての Disk を同期する
func (h *Handler) Shutdown() error {
	if err := h.BufferPool.FlushPage(); err != nil {
		return err
	}
	return h.syncAllDisks()
}

// syncAllDisks はカタログと全テーブルの Disk を同期する
func (h *Handler) syncAllDisks() error {
	// カタログの Disk を同期
	catalogDisk, err := h.BufferPool.GetDisk(page.FileId(0))
	if err != nil {
		return err
	}
	if err := catalogDisk.Sync(); err != nil {
		return err
	}

	// 各テーブルの Disk を同期
	for _, table := range h.Catalog.GetAllTables() {
		dm, err := h.BufferPool.GetDisk(table.FileId)
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
func (h *Handler) RegisterDmToBp(fileId page.FileId, tableName string) error {
	path := filepath.Join(h.baseDirectory, fmt.Sprintf("%s.db", tableName))
	dm, err := file.NewDisk(fileId, path)
	if err != nil {
		return err
	}
	h.BufferPool.RegisterDisk(fileId, dm)
	return nil
}

// newHandler は Handler を初期化する
func newHandler() (*Handler, error) {
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

	return &Handler{
		BufferPool:    bp,
		Catalog:       catalog,
		undoLog:       undoLog,
		trxManager:    transaction.NewManager(undoLog),
		baseDirectory: dataDir,
	}, nil
}

// initCatalog はカタログを初期化する
func initCatalog(baseDir string, bp *buffer.BufferPool) (*dictionary.Catalog, error) {
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

	var cat *dictionary.Catalog
	if catalogExists {
		// 既存のカタログを開く
		cat, err = dictionary.NewCatalog(bp)
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
		cat, err = dictionary.CreateCatalog(bp)
		if err != nil {
			return nil, err
		}
	}

	return cat, nil
}

// registerTableDisks はカタログに含まれるテーブルの Disk を登録する
func registerTableDisks(cat *dictionary.Catalog, baseDir string, bp *buffer.BufferPool) error {
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
