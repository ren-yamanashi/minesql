package planner

import (
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/engine"
	"minesql/internal/executor"
)

func Start(trxId engine.TrxId, stmt ast.Statement) (executor.Executor, error) {
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		ctn := NewCreateTable(s)
		return ctn.Build()
	case *ast.InsertStmt:
		ip := NewInsert(s)
		return ip.Build(trxId)
	case *ast.SelectStmt:
		sp := NewSelect(s)
		return sp.Build()
	case *ast.DeleteStmt:
		dp := NewDelete(s)
		return dp.Build(trxId)
	case *ast.UpdateStmt:
		up := NewUpdate(s)
		return up.Build(trxId)
	default:
		return nil, fmt.Errorf("unsupported statement: %T", s)
	}
}
