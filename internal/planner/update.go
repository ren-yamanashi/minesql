package planner

import (
	"errors"
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
	sm := engine.Get()
	tblMeta, err := sm.Catalog.GetTableMetadataByName(up.Stmt.Table.TableName)
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

	return executor.NewUpdate(up.Stmt.Table.TableName, setColumns, up.Iterator), nil
}
