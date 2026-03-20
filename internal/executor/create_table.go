package executor

import (
	"minesql/internal/access"
	"minesql/internal/catalog"
	"minesql/internal/engine"
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
	sm := engine.Get()

	// FileId を採番
	fileId, err := sm.Catalog.AllocateFileId(sm.BufferPool)
	if err != nil {
		return nil, err
	}

	// Disk を登録
	err = sm.RegisterDmToBp(fileId, ct.tableName)
	if err != nil {
		return nil, err
	}

	// テーブルの metaPageId を設定
	metaPageId, err := sm.BufferPool.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}

	// 各 UniqueIndex の metaPageId を設定
	uniqueIndexes := make([]*access.UniqueIndexAccessMethod, len(ct.indexParams))
	for i, indexParam := range ct.indexParams {
		indexMetaPageId, err := sm.BufferPool.AllocatePageId(fileId)
		if err != nil {
			return nil, err
		}
		uniqueIndex := access.NewUniqueIndexAccessMethod(indexParam.Name, indexParam.ColName, indexMetaPageId, indexParam.SecondaryKey)
		err = uniqueIndex.Create(sm.BufferPool)
		if err != nil {
			return nil, err
		}
		uniqueIndexes[i] = uniqueIndex
	}

	// テーブルを作成
	tbl := access.NewTableAccessMethod(ct.tableName, metaPageId, ct.primaryKeyCount, uniqueIndexes)
	err = tbl.Create(sm.BufferPool)
	if err != nil {
		return nil, err
	}

	// カラムのメタデータを作成
	colMeta := make([]*catalog.ColumnMetadata, len(ct.columnParams))
	for i, colParam := range ct.columnParams {
		colMeta[i] = catalog.NewColumnMetadata(fileId, colParam.Name, uint16(i), colParam.Type)
	}

	// インデックスのメタデータを作成
	idxMeta := make([]*catalog.IndexMetadata, len(ct.indexParams))
	for i, index := range uniqueIndexes {
		idxMeta[i] = catalog.NewIndexMetadata(fileId, index.Name, index.ColName, catalog.IndexTypeUnique, index.MetaPageId)
	}

	// テーブルメタデータを作成
	tblMeta := catalog.NewTableMetadata(fileId, ct.tableName, uint8(len(ct.columnParams)), ct.primaryKeyCount, colMeta, idxMeta, metaPageId)

	// カタログにテーブルを登録
	err = sm.Catalog.Insert(sm.BufferPool, tblMeta)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
