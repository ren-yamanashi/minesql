package planner

import (
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
)

// PlanDelete は DELETE 文の実行計画を構築する
func PlanDelete(trxId handler.TrxId, stmt *ast.DeleteStmt) (executor.Executor, error) {
	hdl := handler.Get()

	// 対象テーブルのメタデータを取得
	tblMeta, ok := hdl.Catalog.GetTableMetaByName(stmt.From.TableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", stmt.From.TableName)
	}

	// WHERE 句を元に検索用の Executor を構築 (Current Read: 最新バージョンを読む)
	rv := access.NewReadView(0, nil, ^uint64(0))
	vr := access.NewVersionReader(nil)
	search := NewSearch(rv, vr, tblMeta, stmt.Where)
	iterator, err := search.Build()
	if err != nil {
		return nil, err
	}

	// テーブルを取得
	tbl, err := hdl.GetTable(stmt.From.TableName)
	if err != nil {
		return nil, err
	}

	return executor.NewDelete(trxId, tbl, iterator), nil
}
