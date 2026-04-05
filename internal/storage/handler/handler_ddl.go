package handler

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/dictionary"
)

// CreateIndexParam はインデックス作成パラメータ
type CreateIndexParam struct {
	Name    string // インデックス名
	ColName string // インデックスを構成するカラム名
	UkIdx   uint16 // ユニークキーに含めるカラムのインデックス (0 始まりの列番号)
}

// CreateColumnParam はカラム作成パラメータ
type CreateColumnParam struct {
	Name string
	Type ColumnType
}

// CreateTable はテーブルを新規作成し、カタログに登録する
func (h *Handler) CreateTable(tableName string, pkCount uint8, idxParams []CreateIndexParam, colParams []CreateColumnParam) error {
	// FileId を採番
	fileId, err := h.Catalog.AllocateFileId(h.BufferPool)
	if err != nil {
		return err
	}

	// Disk を登録
	if err := h.RegisterDmToBp(fileId, tableName); err != nil {
		return err
	}

	// テーブルの metaPageId を設定
	metaPageId, err := h.BufferPool.AllocatePageId(fileId)
	if err != nil {
		return err
	}

	// 各 UniqueIndex を作成
	uniqueIndexes := make([]*access.UniqueIndex, len(idxParams))
	for i, param := range idxParams {
		indexMetaPageId, err := h.BufferPool.AllocatePageId(fileId)
		if err != nil {
			return err
		}
		uniqueIndex := access.NewUniqueIndex(param.Name, param.ColName, indexMetaPageId, param.UkIdx, pkCount)
		if err := uniqueIndex.Create(h.BufferPool); err != nil {
			return err
		}
		uniqueIndexes[i] = uniqueIndex
	}

	// テーブルを作成
	tbl := access.NewTable(tableName, metaPageId, pkCount, uniqueIndexes, nil)
	if err := tbl.Create(h.BufferPool); err != nil {
		return err
	}

	// インデックスメタデータを作成
	idxMeta := make([]*dictionary.IndexMeta, len(idxParams))
	for i, idx := range uniqueIndexes {
		idxMeta[i] = dictionary.NewIndexMeta(fileId, idx.Name, idx.ColName, dictionary.IndexTypeUnique, idx.MetaPageId)
	}

	// カラムメタデータを作成
	colMeta := make([]*dictionary.ColumnMeta, len(colParams))
	for i, col := range colParams {
		colMeta[i] = dictionary.NewColumnMeta(fileId, col.Name, uint16(i), col.Type)
	}

	// テーブルメタデータを作成してカタログに登録
	tblMeta := dictionary.NewTableMeta(fileId, tableName, uint8(len(colParams)), pkCount, colMeta, idxMeta, metaPageId)
	return h.Catalog.Insert(h.BufferPool, tblMeta)
}
