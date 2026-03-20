package planner

import (
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
)

func PlanStart(stmt ast.Statement) (executor.Executor, error) {
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		ctn := NewCreateTable(s)
		return ctn.Build()
	case *ast.InsertStmt:
		ip := NewInsert(s)
		return ip.Build()
	case *ast.SelectStmt:
		search := NewSearch(s.From.TableName, s.Where)
		searchExec, err := search.Build()
		if err != nil {
			return nil, err
		}
		sp := NewSelect(s, searchExec)
		return sp.Build()
	case *ast.DeleteStmt:
		search := NewSearch(s.From.TableName, s.Where)
		searchExec, err := search.Build()
		if err != nil {
			return nil, err
		}
		dp := NewDelete(s, searchExec)
		return dp.Build()
	case *ast.UpdateStmt:
		search := NewSearch(s.Table.TableName, s.Where)
		searchExec, err := search.Build()
		if err != nil {
			return nil, err
		}
		up := NewUpdate(s, searchExec)
		return up.Build()
	default:
		return nil, fmt.Errorf("unsupported statement: %T", s)
	}
}
