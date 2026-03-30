package executor

import "minesql/internal/storage/handler"

// CreateTable はテーブルを作成する
type CreateTable struct {
	tableName       string
	primaryKeyCount uint8
	indexParams     []handler.IndexParam
	columnParams    []handler.ColumnParam
}

func NewCreateTable(tableName string, primaryKeyCount uint8, indexParams []handler.IndexParam, columnParams []handler.ColumnParam) *CreateTable {
	if indexParams == nil {
		indexParams = []handler.IndexParam{}
	}
	if columnParams == nil {
		columnParams = []handler.ColumnParam{}
	}
	return &CreateTable{
		tableName:       tableName,
		primaryKeyCount: primaryKeyCount,
		indexParams:     indexParams,
		columnParams:    columnParams,
	}
}

func (ct *CreateTable) Next() (Record, error) {
	e := handler.Get()
	if err := e.CreateTable(ct.tableName, ct.primaryKeyCount, ct.indexParams, ct.columnParams); err != nil {
		return nil, err
	}
	return nil, nil
}
