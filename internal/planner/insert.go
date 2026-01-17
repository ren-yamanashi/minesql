package planner

import (
	"errors"
	"minesql/internal/executor"
	"minesql/internal/planner/ast/literal"
	"minesql/internal/planner/ast/statement"
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
	colNames := []string{}
	for _, col := range ip.Stmt.Cols {
		colNames = append(colNames, col.ColName)
	}
	if len(colNames) == 0 {
		return nil, errors.New("column names cannot be empty")
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
		if len(record) != len(colNames) {
			return nil, errors.New("number of values does not match number of columns")
		}
		records = append(records, record)
	}
	if len(records) == 0 {
		return nil, errors.New("records cannot be empty")
	}

	return executor.NewInsert(ip.Stmt.Table.TableName, colNames, records), nil
}
