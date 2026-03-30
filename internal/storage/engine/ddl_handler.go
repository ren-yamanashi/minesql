package engine

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/catalog"
)

// IndexParam はインデックス作成パラメータ
type IndexParam struct {
	Name         string
	ColName      string
	SecondaryKey uint16
}

// ColumnParam はカラム作成パラメータ
type ColumnParam struct {
	Name string
	Type ColumnType
}

// CreateTable はテーブルを新規作成し、カタログに登録する
func (e *Engine) CreateTable(tableName string, primaryKeyCount uint8, indexParams []IndexParam, columnParams []ColumnParam) error {
	// FileId を採番
	fileId, err := e.Catalog.AllocateFileId(e.BufferPool)
	if err != nil {
		return err
	}

	// Disk を登録
	if err := e.RegisterDmToBp(fileId, tableName); err != nil {
		return err
	}

	// テーブルの metaPageId を設定
	metaPageId, err := e.BufferPool.AllocatePageId(fileId)
	if err != nil {
		return err
	}

	// 各 UniqueIndex を作成
	uniqueIndexes := make([]*access.UniqueIndexAccessMethod, len(indexParams))
	for i, param := range indexParams {
		indexMetaPageId, err := e.BufferPool.AllocatePageId(fileId)
		if err != nil {
			return err
		}
		uniqueIndex := access.NewUniqueIndexAccessMethod(param.Name, param.ColName, indexMetaPageId, param.SecondaryKey)
		if err := uniqueIndex.Create(e.BufferPool); err != nil {
			return err
		}
		uniqueIndexes[i] = uniqueIndex
	}

	// テーブルを作成
	tbl := access.NewTableAccessMethod(tableName, metaPageId, primaryKeyCount, uniqueIndexes)
	if err := tbl.Create(e.BufferPool); err != nil {
		return err
	}

	// インデックスメタデータを作成
	idxMeta := make([]*catalog.IndexMetadata, len(indexParams))
	for i, idx := range uniqueIndexes {
		idxMeta[i] = catalog.NewIndexMetadata(fileId, idx.Name, idx.ColName, catalog.IndexTypeUnique, idx.MetaPageId)
	}

	// カラムメタデータを作成
	colMeta := make([]*catalog.ColumnMetadata, len(columnParams))
	for i, col := range columnParams {
		colMeta[i] = catalog.NewColumnMetadata(fileId, col.Name, uint16(i), col.Type)
	}

	// テーブルメタデータを作成してカタログに登録
	tblMeta := catalog.NewTableMetadata(fileId, tableName, uint8(len(columnParams)), primaryKeyCount, colMeta, idxMeta, metaPageId)
	return e.Catalog.Insert(e.BufferPool, tblMeta)
}
