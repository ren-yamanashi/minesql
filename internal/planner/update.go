package planner

import (
	"errors"
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/handler"
)

type Update struct {
	Stmt *ast.UpdateStmt
}

func NewUpdate(stmt *ast.UpdateStmt) *Update {
	return &Update{
		Stmt: stmt,
	}
}

func (up *Update) Build(trxId handler.TrxId) (executor.Executor, error) {
	e := handler.Get()

	// 対象テーブルのメタデータを取得
	tblMeta, ok := e.Catalog.GetTableMetadataByName(up.Stmt.Table.TableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", up.Stmt.Table.TableName)
	}

	// カラム名からカラム位置へのマッピングを作成
	colPosMap := make(map[string]uint16)
	for _, colMeta := range tblMeta.Cols {
		colPosMap[colMeta.Name] = colMeta.Pos
	}

	// SetClause を Executor の SetColumn に変換
	var setColumns []executor.SetColumn
	for _, setClause := range up.Stmt.SetClauses {
		pos, ok := colPosMap[setClause.Column.ColName]
		if !ok {
			return nil, errors.New("column does not exist: " + setClause.Column.ColName)
		}
		setColumns = append(setColumns, executor.SetColumn{
			Pos:   pos,
			Value: setClause.Value.ToBytes(),
		})
	}

	// WHERE 句を元に検索用の Executor を構築
	search := NewSearch(tblMeta, up.Stmt.Where)
	iterator, err := search.Build()
	if err != nil {
		return nil, err
	}

	// テーブルを取得
	rawTbl, err := tblMeta.GetTable()
	if err != nil {
		return nil, err
	}
	tbl := handler.NewTableHandler(rawTbl)

	return executor.NewUpdate(trxId, tbl, setColumns, iterator), nil
}
