package planner

import (
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/engine"
)

type Select struct {
	Stmt *ast.SelectStmt
}

func NewSelect(stmt *ast.SelectStmt) *Select {
	return &Select{
		Stmt: stmt,
	}
}

func (sp *Select) Build() (executor.Executor, error) {
	e := engine.Get()

	// 対象テーブルのメタデータを取得
	tblMeta, ok := e.Catalog.GetTableMetadataByName(sp.Stmt.From.TableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", sp.Stmt.From.TableName)
	}

	// WHERE 句を元に検索用の Executor を構築
	search := NewSearch(tblMeta, sp.Stmt.Where)
	iterator, err := search.Build()
	if err != nil {
		return nil, err
	}

	// SELECT 句を元に取得するカラムの位置を特定
	// NOTE: 現状は SELECT * のみサポートしているため、テーブルの全カラムを取得する
	var colPos []uint16
	for _, colMeta := range tblMeta.Cols {
		colPos = append(colPos, colMeta.Pos)
	}
	return executor.NewProject(iterator, colPos), nil
}
