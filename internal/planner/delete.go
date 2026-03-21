package planner

import (
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/engine"
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
	e := engine.Get()

	tblMeta, ok := e.Catalog.GetTableMetadataByName(dp.Stmt.From.TableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", dp.Stmt.From.TableName)
	}

	tbl, err := tblMeta.GetTable()
	if err != nil {
		return nil, err
	}

	return executor.NewDelete(tbl, dp.Iterator), nil
}
