package planner

import (
	"minesql/internal/ast"
	"minesql/internal/executor"
)

type Delete struct {
	Stmt     *ast.DeleteStmt
	Iterator executor.RecordIterator
}

func NewDelete(stmt *ast.DeleteStmt, iterator executor.RecordIterator) *Delete {
	return &Delete{
		Stmt:     stmt,
		Iterator: iterator,
	}
}

func (dp *Delete) Build() (executor.Mutator, error) {
	return executor.NewDelete(dp.Stmt.From.TableName, dp.Iterator), nil
}
