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
	case *statement.InsertStmt:
		ip := NewInsertPlanner(s)
		return ip.Next()
	case *statement.SelectStmt:
		sp := NewSelectPlanner(s)
		return sp.Next()
	default:
		return nil, fmt.Errorf("unsupported statement: %T", s)
	}
}
