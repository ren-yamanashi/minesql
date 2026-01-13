package planner

import (
	"fmt"
	"minesql/internal/executor"
	"minesql/internal/planner/ast/statement"
)

type Node interface {
	Start() executor.Executor
}

type Planner struct{}

func NewPlanner() *Planner {
	return &Planner{}
}

func (p *Planner) PlanStart(stmt statement.Statement) (executor.Executor, error) {
	switch s := stmt.(type) {
	case *statement.CreateTableStmt:
		ctn := NewCreateTableNode(s)
		return ctn.Next()
	default:
		return nil, fmt.Errorf("unsupported statement: %T", s)
	}
}
