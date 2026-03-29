package planner

import (
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
)

func Start(trx *executor.Transaction, stmt ast.Statement) (executor.Executor, error) {
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		ctn := NewCreateTable(s)
		return ctn.Build()
	case *ast.InsertStmt:
		ip := NewInsert(s)
		return ip.Build(trx)
	case *ast.SelectStmt:
		sp := NewSelect(s)
		return sp.Build()
	case *ast.DeleteStmt:
		dp := NewDelete(s)
		return dp.Build(trx)
	case *ast.UpdateStmt:
		up := NewUpdate(s)
		return up.Build(trx)
	default:
		return nil, fmt.Errorf("unsupported statement: %T", s)
	}
}
