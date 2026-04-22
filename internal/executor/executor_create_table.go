package executor

import "minesql/internal/storage/handler"

// CreateTable はテーブルを作成する
type CreateTable struct {
	tableName        string                          // 作成するテーブル名
	pkCount          uint8                           // 主キーのカラム数
	indexParams      []handler.CreateIndexParam      // 作成するインデックスの情報
	columnParams     []handler.CreateColumnParam     // 作成するカラムの情報
	constraintParams []handler.CreateConstraintParam // 作成する外部キー制約の情報
}

func NewCreateTable(tableName string, pkCount uint8, indexParams []handler.CreateIndexParam, columnParams []handler.CreateColumnParam, constraintParams []handler.CreateConstraintParam) *CreateTable {
	if indexParams == nil {
		indexParams = []handler.CreateIndexParam{}
	}
	if columnParams == nil {
		columnParams = []handler.CreateColumnParam{}
	}
	if constraintParams == nil {
		constraintParams = []handler.CreateConstraintParam{}
	}
	return &CreateTable{
		tableName:        tableName,
		pkCount:          pkCount,
		indexParams:      indexParams,
		columnParams:     columnParams,
		constraintParams: constraintParams,
	}
}

func (ct *CreateTable) Next() (Record, error) {
	hdl := handler.Get()
	if err := hdl.CreateTable(ct.tableName, ct.pkCount, ct.indexParams, ct.columnParams, ct.constraintParams); err != nil {
		return nil, err
	}
	return nil, nil
}
