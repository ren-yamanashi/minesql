package planner

import (
	"minesql/internal/executor"
	"minesql/internal/planner/ast/statement"
)

type DeletePlanner struct {
	Stmt          *statement.DeleteStmt
	InnerExecutor executor.Executor
}

func NewDeletePlanner(stmt *statement.DeleteStmt, innerExecutor executor.Executor) *DeletePlanner {
	return &DeletePlanner{
		Stmt:          stmt,
		InnerExecutor: innerExecutor,
	}
}

func (dp *DeletePlanner) Next() (executor.Executor, error) {
	return executor.NewDelete(dp.Stmt.From.TableName, dp.InnerExecutor), nil
}
