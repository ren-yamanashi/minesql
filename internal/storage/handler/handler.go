package handler

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"minesql/internal/storage/buffer"
	"minesql/internal/storage/config"
	"minesql/internal/storage/dictionary"
	"minesql/internal/storage/file"
	"minesql/internal/storage/page"
	"minesql/internal/storage/transaction"
)

const ColumnTypeString = dictionary.ColumnTypeString

type TrxId = transaction.TrxId
type UndoLog = transaction.UndoLog
type TableMetadata = dictionary.TableMeta
type IndexMetadata = dictionary.IndexMeta
type ColumnType = dictionary.ColumnType
type TableStatistics = dictionary.TableStats
type IndexStatistics = dictionary.IndexStats
type ColumnStatistics = dictionary.ColumnStats

var (
	hdl  *Handler
	once sync.Once
)

// Handler はストレージ層のリソースの管理を行う
type Handler struct {
	BufferPool     *buffer.BufferPool
	Catalog        *dictionary.Catalog
	StatsCollector *dictionary.StatsCollector
	undoLog        *transaction.UndoLog
	trxManager     *transaction.Manager
	baseDirectory  string
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

// Reset はグローバルな Handler の状態をリセットする
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
		BufferPool:     bp,
		Catalog:        catalog,
		StatsCollector: dictionary.NewStatsCollector(bp),
		undoLog:        undoLog,
		trxManager:     transaction.NewManager(undoLog),
		baseDirectory:  dataDir,
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

// registerTableDisks はカタログに含まれるテーブルの Disk を BufferPool に登録する
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
