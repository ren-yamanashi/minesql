package planner

import (
	"errors"
	"fmt"
	"minesql/internal/executor"
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/statement"
	"minesql/internal/storage"
	"minesql/internal/storage/access/catalog"
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
	sm := storage.GetStorageManager()

	if sp.Stmt.From.TableName == "" {
		return nil, errors.New("table name cannot be empty")
	}

	tblMeta, err := sm.Catalog.GetTableMetadataByName(sp.Stmt.From.TableName)
	if err != nil {
		return nil, err
	}

	// WHERE 句が設定されていない場合フルテーブルスキャンを実行
	if !sp.Stmt.Where.IsSet {
		return executor.NewSearchTable(
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
		return sp.planForBinaryExpr(tblMeta, *e)
	default:
		return nil, errors.New("unsupported WHERE condition type")
	}
}

func (sp *SelectPlanner) planForBinaryExpr(tblMeta *catalog.TableMetadata, expr expression.BinaryExpr) (executor.Executor, error) {
	switch rhs := expr.Right.(type) {
	case *expression.RhsLiteral:
		if _, ok := tblMeta.GetColByName(expr.Left.ColName); !ok {
			return nil, errors.New("column " + expr.Left.ColName + " does not exist in table " + sp.Stmt.From.TableName)
		}
		// カラムがインデックスの場合
		if idxMeta, ok := tblMeta.GetIndexByColName(expr.Left.ColName); ok {
			cond, err := sp.OperatorToCondition(0, rhs)
			if err != nil {
				return nil, err
			} 
			return executor.NewSearchIndex(
				sp.Stmt.From.TableName,
				idxMeta.Name,
				executor.RecordSearchModeKey{Key: [][]byte{rhs.Literal.ToBytes()}},
				cond,
			), nil
		}
		// カラムがインデックスでない場合
		colMeta, ok := tblMeta.GetColByName(expr.Left.ColName)
		if !ok {
			return nil, errors.New("column " + expr.Left.ColName + " does not exist in table " + sp.Stmt.From.TableName)
		}
		cond, err := sp.OperatorToCondition(int(colMeta.Pos), rhs)
		if err != nil {
			return nil, err
		}
		return executor.NewSearchTable(
			sp.Stmt.From.TableName,
			executor.RecordSearchModeKey{Key: [][]byte{rhs.Literal.ToBytes()}},
			cond,
		), nil
	case *expression.RhsExpr:
		// TODO
		return nil, nil
	default:
		return nil, errors.New("unsupported binary expression in WHERE clause")
	}
}

func (sp *SelectPlanner) OperatorToCondition(pos int, rhs *expression.RhsLiteral) (func(executor.Record) bool, error) {
	switch op := sp.Stmt.Where.Condition.(*expression.BinaryExpr).Operator; op {
	case "=":
		return func(record executor.Record) bool {
			return string(record[pos]) == rhs.Literal.ToString()
		}, nil
	case "!=":
		return func(record executor.Record) bool {
			return string(record[pos]) != rhs.Literal.ToString()
		}, nil
	case "<":
		return func(record executor.Record) bool {
			return string(record[pos]) < rhs.Literal.ToString()
		}, nil
	case "<=":
		return func(record executor.Record) bool {
			return string(record[pos]) <= rhs.Literal.ToString()
		}, nil
	case ">":
		return func(record executor.Record) bool {
			return string(record[pos]) > rhs.Literal.ToString()
		}, nil
	case ">=":
		return func(record executor.Record) bool {
			return string(record[pos]) >= rhs.Literal.ToString()
		}, nil
	default:
		return nil, fmt.Errorf("unsupported operator in WHERE clause: %s", op)
	}
}
