package executor

import "minesql/internal/storage/handler"

// CreateTable はテーブルを作成する
type CreateTable struct {
	tableName    string                      // 作成するテーブル名
	pkCount      uint8                       // 主キーのカラム数
	indexParams  []handler.CreateIndexParam  // 作成するインデックスの情報
	columnParams []handler.CreateColumnParam // 作成するカラムの情報
}

func NewCreateTable(tableName string, pkCount uint8, indexParams []handler.CreateIndexParam, columnParams []handler.CreateColumnParam) *CreateTable {
	if indexParams == nil {
		indexParams = []handler.CreateIndexParam{}
	}
	if columnParams == nil {
		columnParams = []handler.CreateColumnParam{}
	}
	return &CreateTable{
		tableName:    tableName,
		pkCount:      pkCount,
		indexParams:  indexParams,
		columnParams: columnParams,
	}
}

func (ct *CreateTable) Next() (Record, error) {
	e := handler.Get()
	if err := e.CreateTable(ct.tableName, ct.pkCount, ct.indexParams, ct.columnParams); err != nil {
		return nil, err
	}
	return nil, nil
}
