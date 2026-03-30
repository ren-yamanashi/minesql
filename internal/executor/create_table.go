package executor

import (
	"minesql/internal/engine"
	"minesql/internal/storage/access"
	"minesql/internal/storage/catalog"
)

type ColumnParam struct {
	Name string
	Type catalog.ColumnType
}

type IndexParam struct {
	Name         string
	ColName      string
	SecondaryKey uint16
}

// CreateTable はテーブルを作成する
type CreateTable struct {
	tableName       string
	primaryKeyCount uint8
	indexParams     []*IndexParam
	columnParams    []*ColumnParam
}

func NewCreateTable(tableName string, primaryKeyCount uint8, indexParams []*IndexParam, columnParams []*ColumnParam) *CreateTable {
	if indexParams == nil {
		indexParams = []*IndexParam{}
	}
	if columnParams == nil {
		columnParams = []*ColumnParam{}
	}
	return &CreateTable{
		tableName:       tableName,
		primaryKeyCount: primaryKeyCount,
		indexParams:     indexParams,
		columnParams:    columnParams,
	}
}

func (ct *CreateTable) Next() (Record, error) {
	e := engine.Get()

	// FileId を採番
	fileId, err := e.Catalog.AllocateFileId(e.BufferPool)
	if err != nil {
		return nil, err
	}

	// Disk を登録
	err = e.RegisterDmToBp(fileId, ct.tableName)
	if err != nil {
		return nil, err
	}

	// テーブルの metaPageId を設定
	metaPageId, err := e.BufferPool.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}

	// 各 UniqueIndex の metaPageId を設定
	uniqueIndexes := make([]*access.UniqueIndexAccessMethod, len(ct.indexParams))
	for i, indexParam := range ct.indexParams {
		indexMetaPageId, err := e.BufferPool.AllocatePageId(fileId)
		if err != nil {
			return nil, err
		}
		uniqueIndex := access.NewUniqueIndexAccessMethod(indexParam.Name, indexParam.ColName, indexMetaPageId, indexParam.SecondaryKey)
		err = uniqueIndex.Create(e.BufferPool)
		if err != nil {
			return nil, err
		}
		uniqueIndexes[i] = uniqueIndex
	}

	// テーブルを作成
	tbl := access.NewTableAccessMethod(ct.tableName, metaPageId, ct.primaryKeyCount, uniqueIndexes)
	err = tbl.Create(e.BufferPool)
	if err != nil {
		return nil, err
	}

	// インデックスのメタデータを作成
	idxMeta := make([]*catalog.IndexMetadata, len(ct.indexParams))
	for i, index := range uniqueIndexes {
		idxMeta[i] = catalog.NewIndexMetadata(fileId, index.Name, index.ColName, catalog.IndexTypeUnique, index.MetaPageId)
	}

	// カラムのメタデータを作成
	colMeta := make([]*catalog.ColumnMetadata, len(ct.columnParams))
	for i, colParam := range ct.columnParams {
		colMeta[i] = catalog.NewColumnMetadata(fileId, colParam.Name, uint16(i), colParam.Type)
	}

	// テーブルメタデータを作成
	tblMeta := catalog.NewTableMetadata(fileId, ct.tableName, uint8(len(ct.columnParams)), ct.primaryKeyCount, colMeta, idxMeta, metaPageId)

	// カタログにテーブルを登録
	err = e.Catalog.Insert(e.BufferPool, tblMeta)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
