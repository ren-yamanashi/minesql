package planner

import (
	"errors"
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/engine"
)

type Insert struct {
	Stmt *ast.InsertStmt
}

func NewInsert(stmt *ast.InsertStmt) *Insert {
	return &Insert{
		Stmt: stmt,
	}
}

func (ip *Insert) Build(trxId engine.TrxId) (executor.Executor, error) {
	if len(ip.Stmt.Cols) == 0 {
		return nil, errors.New("column names cannot be empty")
	}

	if len(ip.Stmt.Values) == 0 {
		return nil, errors.New("records cannot be empty")
	}

	// カラム名の重複チェック
	seenCols := map[string]bool{}
	for _, col := range ip.Stmt.Cols {
		if seenCols[col.ColName] {
			return nil, errors.New("duplicate column name: " + col.ColName)
		}
		seenCols[col.ColName] = true
	}

	// 値の数がカラム数と一致することを確認
	for _, valList := range ip.Stmt.Values {
		if len(valList) != len(ip.Stmt.Cols) {
			return nil, errors.New("number of values does not match number of columns")
		}
	}

	e := engine.Get()
	tblMeta, ok := e.Catalog.GetTableMetadataByName(ip.Stmt.Table.TableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", ip.Stmt.Table.TableName)
	}
	rawTbl, err := tblMeta.GetTable()
	if err != nil {
		return nil, err
	}
	tbl := engine.NewTableHandler(rawTbl)

	colPosMap := make(map[string]uint16)
	for _, colMeta := range tblMeta.Cols {
		colPosMap[colMeta.Name] = colMeta.Pos
	}

	// レコードをテーブルのカラム順序に並び替える
	records := []executor.Record{}
	for _, valList := range ip.Stmt.Values {
		record := make([][]byte, len(tblMeta.Cols))
		for i, val := range valList {
			colName := ip.Stmt.Cols[i].ColName
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
