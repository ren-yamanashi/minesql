package planner

import (
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
)

func PlanSelect(trxId handler.TrxId, stmt *ast.SelectStmt) (executor.Executor, error) {
	hdl := handler.Get()

	// 対象テーブルのメタデータを取得
	tblMeta, ok := hdl.Catalog.GetTableMetaByName(stmt.From.TableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", stmt.From.TableName)
	}

	// WHERE 句を元に検索用の Executor を構築
	rv := hdl.CreateReadView(trxId)
	vr := access.NewVersionReader(hdl.UndoLog())
	search := NewSearch(rv, vr, tblMeta, stmt.Where)
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
