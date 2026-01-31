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

	colNames, err := getColNames(ip.Stmt)
	if err != nil {
		return nil, err
	}

	records := [][][]byte{}
	for _, valList := range ip.Stmt.Values {
		record := [][]byte{}
		for _, val := range valList {
			switch v := val.(type) {
			case *literal.StringLiteral:
				record = append(record, []byte(v.Value))
			default:
				return nil, errors.New("unsupported literal type in insert values")
			}
		}
		records = append(records, record)
	}

	return executor.NewInsert(ip.Stmt.Table.TableName, colNames, records), nil
}

// INSERT 文のカラム名を取得する
// INSERT で指定されるカラムの順序は、`CREATE TABLE` の時の順序と一致している必要はない
func getColNames(stmt *statement.InsertStmt) ([]string, error) {
	sm := storage.GetStorageManager()
	tblMeta, err := sm.Catalog.GetTableMetadataByName(stmt.Table.TableName)
	if err != nil {
		return nil, err
	}

	colNames := make([]string, len(stmt.Cols))
	for _, col := range stmt.Cols {
		colMeta, ok := tblMeta.GetColByName(col.ColName)
		if !ok {
			return nil, errors.New("column does not exist: " + col.ColName)
		}
		colNames[colMeta.Pos] = colMeta.Name
	}
	return colNames, nil
}
