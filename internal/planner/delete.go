package planner

import (
	"minesql/internal/ast"
	"minesql/internal/executor"
)

type Delete struct {
	Stmt     *ast.DeleteStmt
	Iterator executor.Executor
}

func NewDelete(stmt *ast.DeleteStmt, iterator executor.Executor) *Delete {
	return &Delete{
		Stmt:     stmt,
		Iterator: iterator,
	}
}

func (dp *Delete) Build() (executor.Executor, error) {
	return executor.NewDelete(dp.Stmt.From.TableName, dp.Iterator), nil
}
