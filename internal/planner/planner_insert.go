package planner

import (
	"errors"
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/handler"
)

// PlanInsert は INSERT 文の実行計画を構築する
func PlanInsert(trxId handler.TrxId, stmt *ast.InsertStmt) (executor.Executor, error) {
	if len(stmt.Cols) == 0 {
		return nil, errors.New("column names cannot be empty")
	}

	if len(stmt.Values) == 0 {
		return nil, errors.New("records cannot be empty")
	}

	// カラム名の重複チェック
	seenCols := map[string]bool{}
	for _, col := range stmt.Cols {
		if seenCols[col.ColName] {
			return nil, errors.New("duplicate column name: " + col.ColName)
		}
		seenCols[col.ColName] = true
	}

	// 値の数がカラム数と一致することを確認
	for _, valList := range stmt.Values {
		if len(valList) != len(stmt.Cols) {
			return nil, errors.New("number of values does not match number of columns")
		}
	}

	hdl := handler.Get()
	tblMeta, ok := hdl.Catalog.GetTableMetaByName(stmt.Table.TableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", stmt.Table.TableName)
	}
	tbl, err := tblMeta.GetTable()
	if err != nil {
		return nil, err
	}

	colPosMap := make(map[string]uint16)
	for _, colMeta := range tblMeta.Cols {
		colPosMap[colMeta.Name] = colMeta.Pos
	}

	// レコードをテーブルのカラム順序に並び替える
	records := []executor.Record{}
	for _, valList := range stmt.Values {
		record := make([][]byte, len(tblMeta.Cols))
		for i, val := range valList {
			colName := stmt.Cols[i].ColName
			pos, ok := colPosMap[colName]
			if !ok {
				return nil, errors.New("column does not exist: " + colName)
			}
			switch v := val.(type) {
			case *ast.StringLiteral:
				record[pos] = []byte(v.Value)
			default:
				return nil, errors.New("unsupported literal type in insert values")
			}
		}
		records = append(records, record)
	}

	return executor.NewInsert(trxId, tbl, records), nil
}
