package planner

import (
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/engine"
	"minesql/internal/executor"
)

type Delete struct {
	Stmt *ast.DeleteStmt
}

func NewDelete(stmt *ast.DeleteStmt) *Delete {
	return &Delete{
		Stmt: stmt,
	}
}

func (dp *Delete) Build() (executor.Executor, error) {
	e := engine.Get()

	// 対象テーブルのメタデータを取得
	tblMeta, ok := e.Catalog.GetTableMetadataByName(dp.Stmt.From.TableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", dp.Stmt.From.TableName)
	}

	// WHERE 句を元に検索用の Executor を構築
	search := NewSearch(tblMeta, dp.Stmt.Where)
	iterator, err := search.Build()
	if err != nil {
		return nil, err
	}

	// テーブルを取得
	tbl, err := tblMeta.GetTable()
	if err != nil {
		return nil, err
	}

	return executor.NewDelete(tbl, iterator), nil
}
