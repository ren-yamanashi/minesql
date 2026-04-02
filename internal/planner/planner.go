package planner

import (
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/handler"
)

func Start(trxId handler.TrxId, stmt ast.Statement) (executor.Executor, error) {
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		return PlanCreateTable(s)
	case *ast.InsertStmt:
		return PlanInsert(trxId, s)
	case *ast.SelectStmt:
		return PlanSelect(s)
	case *ast.DeleteStmt:
		return PlanDelete(trxId, s)
	case *ast.UpdateStmt:
		return PlanUpdate(trxId, s)
	default:
		return nil, fmt.Errorf("unsupported statement: %T", s)
	}
}
