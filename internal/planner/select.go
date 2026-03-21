package planner

import (
	"errors"
	"minesql/internal/ast"
	"minesql/internal/engine"
	"minesql/internal/executor"
)

type Select struct {
	Stmt     *ast.SelectStmt
	iterator executor.Executor
}

func NewSelect(stmt *ast.SelectStmt, iterator executor.Executor) *Select {
	return &Select{
		Stmt:     stmt,
		iterator: iterator,
	}
}

func (sp *Select) Build() (executor.Executor, error) {
	e := engine.Get()

	_, ok := e.Catalog.GetTableMetadataByName(sp.Stmt.From.TableName)
	if !ok {
		return nil, errors.New("table not found: " + sp.Stmt.From.TableName)
	}

	// 現時点では検索 RecordIterator をそのまま返す
	// 将来的にカラム射影などを追加する場合はここに処理を追加する
	return sp.iterator, nil
}
