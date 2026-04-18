package planner

import (
	"errors"
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
)

func PlanUpdate(trxId handler.TrxId, stmt *ast.UpdateStmt) (executor.Executor, error) {
	hdl := handler.Get()

	// 対象テーブルのメタデータを取得
	tblMeta, ok := hdl.Catalog.GetTableMetaByName(stmt.Table.TableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", stmt.Table.TableName)
	}

	// カラム名からカラム位置へのマッピングを作成
	colPosMap := make(map[string]uint16)
	for _, colMeta := range tblMeta.Cols {
		colPosMap[colMeta.Name] = colMeta.Pos
	}

	// SetClause を Executor の SetColumn に変換
	var setColumns []executor.SetColumn
	for _, setClause := range stmt.SetClauses {
		pos, ok := colPosMap[setClause.Column.ColName]
		if !ok {
			return nil, errors.New("column does not exist: " + setClause.Column.ColName)
		}
		setColumns = append(setColumns, executor.SetColumn{
			Pos:   pos,
			Value: setClause.Value.ToBytes(),
		})
	}

	// WHERE 句を元に検索用の Executor を構築 (Current Read: 最新バージョンを読む)
	rv := access.NewReadView(0, nil, ^uint64(0))
	vr := access.NewVersionReader(nil)
	search := NewSearch(rv, vr, tblMeta, stmt.Where, hdl.BufferPool)
	iterator, err := search.Build()
	if err != nil {
		return nil, err
	}

	// テーブルを取得
	tbl, err := hdl.GetTable(stmt.Table.TableName)
	if err != nil {
		return nil, err
	}

	return executor.NewUpdate(trxId, tbl, setColumns, iterator), nil
}
