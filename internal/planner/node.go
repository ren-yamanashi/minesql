package planner

import (
	"fmt"
	"minesql/internal/executor"
	"minesql/internal/planner/ast/statement"
)

type PlannerNode interface {
	Start() executor.Executor
}

func PlanStart(stmt statement.Statement) (executor.Executor, error) {
	switch s := stmt.(type) {
	case *statement.CreateTableStmt:
		ctn := NewCreateTableNode(s)
		return ctn.Next()
	default:
		return nil, fmt.Errorf("unsupported statement: %T", s)
	}
}
