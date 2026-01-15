package planner

import (
	"minesql/internal/executor"
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
	return nil, nil
}
