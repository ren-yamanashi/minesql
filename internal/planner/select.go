package planner

import (
	"errors"
	"minesql/internal/executor"
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/statement"
	"minesql/internal/storage"
)

type SelectPlanner struct {
	Stmt *statement.SelectStmt
}

func NewSelectPlanner(stmt *statement.SelectStmt) *SelectPlanner {
	return &SelectPlanner{
		Stmt: stmt,
	}
}

func (sp *SelectPlanner) Next() (executor.Executor, error) {
	engine := storage.GetStorageManager()
	cat := engine.GetCatalog()

	if sp.Stmt.From.TableName == "" {
		return nil, errors.New("table name cannot be empty")
	}

	tblMeta, err := cat.GetTableMetadataByName(sp.Stmt.From.TableName)
	if err != nil {
		return nil, err
	}

	// WHERE 句が設定されていない場合フルテーブルスキャンを実行
	if !sp.Stmt.Where.IsSet {
		return executor.NewSequentialScan(
			sp.Stmt.From.TableName,
			executor.RecordSearchModeStart{},
			func(record executor.Record) bool {
				return true // フルテーブルスキャンなので常に true を返す
			},
		), nil
	}

	// WHERE 句が設定されている場合
	switch e := sp.Stmt.Where.Condition.(type) {
	case *expression.BinaryExpr:
		if !tblMeta.HasColumn(e.Left.ColName) {
			return nil, errors.New("column " + e.Left.ColName + " does not exist in table " + sp.Stmt.From.TableName)
		}
		// カラムがインデックスの場合
		if idxMeta, ok := tblMeta.GetIndexByColName(e.Left.ColName); ok {
			return executor.NewIndexScan(
				sp.Stmt.From.TableName,
				idxMeta.Name,
				executor.RecordSearchModeKey{Key: [][]byte{e.Right.ToBytes()}},
				func(secondaryKey executor.Record) bool {
					return string(secondaryKey[0]) == e.Right.ToString()
				},
			), nil
		}
		// カラムがインデックスでない場合
		numOfCols, ok := tblMeta.GetColIndex(e.Left.ColName)
		if !ok {
			return nil, errors.New("column " + e.Left.ColName + " does not exist in table " + sp.Stmt.From.TableName)
		}
		return executor.NewSequentialScan(
			sp.Stmt.From.TableName,
			executor.RecordSearchModeKey{Key: [][]byte{e.Right.ToBytes()}},
			func(record executor.Record) bool {
				return string(record[numOfCols]) == e.Right.ToString()
			},
		), nil
	default:
		return nil, errors.New("unsupported WHERE condition type")
	}
}
