package planner

import (
	"errors"
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/engine"
	"minesql/internal/executor"
)

type Update struct {
	Stmt     *ast.UpdateStmt
	Iterator executor.Executor
}

func NewUpdate(stmt *ast.UpdateStmt, iterator executor.Executor) *Update {
	return &Update{
		Stmt:     stmt,
		Iterator: iterator,
	}
}

func (up *Update) Build() (executor.Executor, error) {
	e := engine.Get()
	tblMeta, ok := e.Catalog.GetTableMetadataByName(up.Stmt.Table.TableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", up.Stmt.Table.TableName)
	}

	tbl, err := tblMeta.GetTable()
	if err != nil {
		return nil, err
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

	return executor.NewUpdate(tbl, setColumns, up.Iterator), nil
}
