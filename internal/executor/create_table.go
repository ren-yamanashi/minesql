package executor

import (
	"io"
	"minesql/internal/storage"
	"minesql/internal/storage/access/table"
)

type IndexParam struct {
	Name         string
	SecondaryKey uint
}

type CreateTable struct {
	tableName       string
	primaryKeyCount int
	indexParams     []*IndexParam
	executed        bool
}

func NewCreateTable(tableName string, primaryKeyCount int, indexParams []*IndexParam) *CreateTable {
	if indexParams == nil {
		indexParams = []*IndexParam{}
	}
	return &CreateTable{
		tableName:       tableName,
		primaryKeyCount: primaryKeyCount,
		indexParams:     indexParams,
		executed:        false,
	}
}

func (ct *CreateTable) Next() (Record, error) {
	if ct.executed {
		return nil, io.EOF
	}
	err := ct.execute()
	if err != nil {
		return nil, err
	}
	ct.executed = true
	return nil, nil
}

func (ct *CreateTable) execute() error {
	engine := storage.GetStorageManager()
	bpm := engine.GetBufferPoolManager()

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
		uniqueIndex := table.NewUniqueIndex(indexParam.Name, indexParam.SecondaryKey)
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
	ct.executed = true
	return nil
}
