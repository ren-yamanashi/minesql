package planner

import (
	"minesql/internal/executor"
	"minesql/internal/planner/ast/statement"
)

type DeletePlanner struct {
	Stmt     *statement.DeleteStmt
	Iterator executor.RecordIterator
}

func NewDeletePlanner(stmt *statement.DeleteStmt, iterator executor.RecordIterator) *DeletePlanner {
	return &DeletePlanner{
		Stmt:     stmt,
		Iterator: iterator,
	}
}

func (dp *DeletePlanner) Next() (executor.Mutator, error) {
	return executor.NewDelete(dp.Stmt.From.TableName, dp.Iterator), nil
}
