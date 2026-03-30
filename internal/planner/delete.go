package planner

import (
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/engine"
)

type Delete struct {
	Stmt *ast.DeleteStmt
}

func NewDelete(stmt *ast.DeleteStmt) *Delete {
	return &Delete{
		Stmt: stmt,
	}
}

func (dp *Delete) Build(trxId engine.TrxId) (executor.Executor, error) {
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
	rawTbl, err := tblMeta.GetTable()
	if err != nil {
		return nil, err
	}
	tbl := engine.NewTableHandler(rawTbl)

	return executor.NewDelete(trxId, tbl, iterator), nil
}
