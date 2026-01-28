package executor

import (
	"minesql/internal/storage"
	"minesql/internal/storage/access/catalog"
	"minesql/internal/storage/access/table"
)

type ColumnParam struct {
	Name string
	Type catalog.ColumnType
}

type IndexParam struct {
	Name         string
	ColName      string
	SecondaryKey uint
}

type CreateTable struct {
	tableName       string
	primaryKeyCount int
	indexParams     []*IndexParam
	columnParams    []*ColumnParam
}

func NewCreateTable(tableName string, primaryKeyCount int, indexParams []*IndexParam, columnParams []*ColumnParam) *CreateTable {
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
	err := ct.execute()
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (ct *CreateTable) execute() error {
	engine := storage.GetStorageManager()
	bpm := engine.GetBufferPoolManager()
	cat := engine.GetCatalog()

	// FileId を割り当て
	fileId := bpm.AllocateFileId()

	// DiskManager を登録
	err := engine.RegisterDmToBpm(fileId, ct.tableName)
	if err != nil {
		return err
	}

	// テーブルの metaPageId を設定
	metaPageId, err := bpm.AllocatePageId(fileId)
	if err != nil {
		return err
	}

	// 各 UniqueIndex の metaPageId を設定
	uniqueIndexes := make([]*table.UniqueIndex, len(ct.indexParams))
	for i, indexParam := range ct.indexParams {
		indexMetaPageId, err := bpm.AllocatePageId(fileId)
		if err != nil {
			return err
		}
		uniqueIndex := table.NewUniqueIndex(indexParam.Name, indexParam.ColName, indexParam.SecondaryKey)
		uniqueIndex.Create(bpm, indexMetaPageId)
		uniqueIndexes[i] = uniqueIndex
	}

	// テーブルを作成
	tbl := table.NewTable(ct.tableName, metaPageId, ct.primaryKeyCount, uniqueIndexes)
	err = tbl.Create(bpm)
	if err != nil {
		return err
	}

	engine.RegisterTable(&tbl)

	// テーブルIDを採番
	tblId, err := cat.AllocateTableId(bpm)
	if err != nil {
		return err
	}

	// カラムのメタデータを作成
	colMeta := make([]catalog.ColumnMetadata, len(ct.columnParams))
	for i, colParam := range ct.columnParams {
		colMeta[i] = catalog.NewColumnMetadata(tblId, colParam.Name, uint16(i), colParam.Type)
	}

	// インデックスのメタデータを作成
	idxMeta := make([]catalog.IndexMetadata, len(ct.indexParams))
	for i, index := range uniqueIndexes {
		idxMeta[i] = catalog.NewIndexMetadata(tblId, index.Name, index.ColName, catalog.IndexTypeUnique, index.MetaPageId)
	}

	// テーブルメタデータを作成
	tblMeta := catalog.NewTableMetadata(tblId, ct.tableName, uint8(len(ct.columnParams)), colMeta, idxMeta, metaPageId)

	// カタログにテーブルを登録
	cat.Insert(bpm, tblMeta)

	return nil
}
