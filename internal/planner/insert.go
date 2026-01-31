package planner

import (
	"errors"
	"minesql/internal/executor"
	"minesql/internal/planner/ast/literal"
	"minesql/internal/planner/ast/statement"
	"minesql/internal/storage"
)

type InsertPlanner struct {
	Stmt *statement.InsertStmt
}

func NewInsertPlanner(stmt *statement.InsertStmt) *InsertPlanner {
	return &InsertPlanner{
		Stmt: stmt,
	}
}

func (ip *InsertPlanner) Next() (executor.Executor, error) {
	if len(ip.Stmt.Cols) == 0 {
		return nil, errors.New("column names cannot be empty")
	}

	if len(ip.Stmt.Values) == 0 {
		return nil, errors.New("records cannot be empty")
	}

	// 値の数がカラム数と一致することを確認
	for _, valList := range ip.Stmt.Values {
		if len(valList) != len(ip.Stmt.Cols) {
			return nil, errors.New("number of values does not match number of columns")
		}
	}

	sm := storage.GetStorageManager()
	tblMeta, err := sm.Catalog.GetTableMetadataByName(ip.Stmt.Table.TableName)
	if err != nil {
		return nil, err
	}

	// テーブルのカラム名を順序通りに取得
	colNames := make([]string, len(tblMeta.Cols))
	for _, colMeta := range tblMeta.Cols {
		colNames[colMeta.Pos] = colMeta.Name
	}

	// INSERT 文で指定されたカラムの位置をマッピング
	colPosMap := make(map[string]uint16)
	for _, colMeta := range tblMeta.Cols {
		colPosMap[colMeta.Name] = colMeta.Pos
	}

	// レコードをテーブルのカラム順序に並び替える
	records := [][][]byte{}
	for _, valList := range ip.Stmt.Values {
		record := make([][]byte, len(tblMeta.Cols))
		for i, val := range valList {
			colName := ip.Stmt.Cols[i].ColName
			pos, ok := colPosMap[colName]
			if !ok {
				return nil, errors.New("column does not exist: " + colName)
			}
			switch v := val.(type) {
			case *literal.StringLiteral:
				record[pos] = []byte(v.Value)
			default:
				return nil, errors.New("unsupported literal type in insert values")
			}
		}
		records = append(records, record)
	}

	return executor.NewInsert(ip.Stmt.Table.TableName, colNames, records), nil
}
