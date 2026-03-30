package executor

import "minesql/internal/storage/engine"

// CreateTable はテーブルを作成する
type CreateTable struct {
	tableName       string
	primaryKeyCount uint8
	indexParams     []engine.IndexParam
	columnParams    []engine.ColumnParam
}

func NewCreateTable(tableName string, primaryKeyCount uint8, indexParams []engine.IndexParam, columnParams []engine.ColumnParam) *CreateTable {
	if indexParams == nil {
		indexParams = []engine.IndexParam{}
	}
	if columnParams == nil {
		columnParams = []engine.ColumnParam{}
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
	if err := e.CreateTable(ct.tableName, ct.primaryKeyCount, ct.indexParams, ct.columnParams); err != nil {
		return nil, err
	}
	return nil, nil
}
