package executor

import (
	"minesql/internal/storage"
	"minesql/internal/storage/access/table"
)

type IndexParam struct {
	Name         string
	SecondaryKey uint
}

type CreateTable struct {
}

func NewCreateTable() *CreateTable {
	return &CreateTable{}
}

func (ct *CreateTable) Execute(tableName string, primaryKeyCount int, indexParams []*IndexParam) error {
	engine := storage.GetStorageEngine()
	bpm := engine.GetBufferPoolManager()

	// FileId を割り当て
	fileId := bpm.AllocateFileId()

	// DiskManager を登録
	err := engine.RegisterDmToBpm(fileId, tableName)
	if err != nil {
		return err
	}

	// テーブルの metaPageId を設定
	metaPageId, err := bpm.AllocatePageId(fileId)
	if err != nil {
		return err
	}

	// 各 UniqueIndex の metaPageId を設定
	uniqueIndexes := make([]*table.UniqueIndex, len(indexParams))
	if indexParams == nil {
		indexParams = []*IndexParam{}
	}
	for i, indexParam := range indexParams {
		indexMetaPageId, err := bpm.AllocatePageId(fileId)
		if err != nil {
			return err
		}
		uniqueIndex := table.NewUniqueIndex(indexParam.Name, indexParam.SecondaryKey)
		uniqueIndex.Create(bpm, indexMetaPageId)
		uniqueIndexes[i] = uniqueIndex
	}

	// テーブルを作成
	tbl := table.NewTable(tableName, metaPageId, primaryKeyCount, uniqueIndexes)
	err = tbl.Create(bpm)
	if err != nil {
		return err
	}

	engine.RegisterTable(&tbl)
	return nil
}

// NOTE: Executor インターフェースを実装するためのダミー実装
func (ct *CreateTable) Next() (Record, error) {
	return nil, nil
}
