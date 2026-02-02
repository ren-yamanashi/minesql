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
	switch lhs := expr.Left.(type) {
	case *expression.LhsColumn:
		colName := lhs.Column.ColName
		switch rhs := expr.Right.(type) {
		case *expression.RhsLiteral:
			if _, ok := tblMeta.GetColByName(colName); !ok {
				return nil, errors.New("column " + colName + " does not exist in table " + sp.Stmt.From.TableName)
			}
			// カラムがインデックスの場合
			if idxMeta, ok := tblMeta.GetIndexByColName(colName); ok {
				cond, err := sp.operatorToCondition(expr.Operator, 0, rhs.Literal.ToString())
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
			colMeta, ok := tblMeta.GetColByName(colName)
			if !ok {
				return nil, errors.New("column " + colName + " does not exist in table " + sp.Stmt.From.TableName)
			}
			cond, err := sp.operatorToCondition(expr.Operator, int(colMeta.Pos), rhs.Literal.ToString())
			if err != nil {
				return nil, err
			}
			return executor.NewSearchTable(
				sp.Stmt.From.TableName,
				executor.RecordSearchModeKey{Key: [][]byte{rhs.Literal.ToBytes()}},
				cond,
			), nil
		default:
			return nil, errors.New("When LHS is a column, RHS must be a literal")
		}
	case *expression.LhsExpr:
		conditions, err := sp.extractConditions(tblMeta, expr)
		if err != nil {
			return nil, err
		}
		return executor.NewSearchTable(
			sp.Stmt.From.TableName,
			executor.RecordSearchModeStart{},
			func(record executor.Record) bool {
				for _, cond := range conditions {
					if !cond(record) {
						return false
					}
				}
				return true
			},
		), nil
	default:
		return nil, errors.New("unsupported LHS type in binary expression")
	}
}

// 再帰的に binary expression から条件関数のリストを抽出する
func (sp *SelectPlanner) extractConditions(tblMeta *catalog.TableMetadata, expr expression.BinaryExpr) ([]func(executor.Record) bool, error) {
	conditions := []func(executor.Record) bool{}

	switch lhs := expr.Left.(type) {
	case *expression.LhsColumn:
		colName := lhs.Column.ColName
		colMeta, ok := tblMeta.GetColByName(colName)
		if !ok {
			return nil, errors.New("column " + colName + " does not exist in table " + sp.Stmt.From.TableName)
		}

		switch rhs := expr.Right.(type) {
		case *expression.RhsLiteral:
			cond, err := sp.operatorToCondition(expr.Operator, int(colMeta.Pos), rhs.Literal.ToString())
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, cond)
		case *expression.RhsExpr:
			return nil, errors.New("When LHS is a column, RHS must be a literal")
		default:
			panic("unsupported RHS type in extractConditions") // 実際にはここには到達しないので errors.New ではなく panic で良い
		}

	case *expression.LhsExpr:
		leftConds, err := sp.extractConditions(tblMeta, *lhs.Expr.(*expression.BinaryExpr))
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, leftConds...)

		switch rhs := expr.Right.(type) {
		case *expression.RhsExpr:
			rightConds, err := sp.extractConditions(tblMeta, *rhs.Expr.(*expression.BinaryExpr))
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, rightConds...)
		case *expression.RhsLiteral:
			return nil, errors.New("When LHS is an expression, RHS cannot be a literal")
		default:
			panic("unsupported RHS type in extractConditions") // 実際にはここには到達しないので errors.New ではなく panic で良い
		}
	default:
		panic("unsupported LHS type in extractConditions") // 実際にはここには到達しないので errors.New ではなく panic で良い
	}

	return conditions, nil
}

func (sp *SelectPlanner) operatorToCondition(operator string, pos int, value string) (func(executor.Record) bool, error) {
	switch operator {
	case "=":
		return func(record executor.Record) bool {
			return string(record[pos]) == value
		}, nil
	case "!=":
		return func(record executor.Record) bool {
			return string(record[pos]) != value
		}, nil
	case "<":
		return func(record executor.Record) bool {
			return string(record[pos]) < value
		}, nil
	case "<=":
		return func(record executor.Record) bool {
			return string(record[pos]) <= value
		}, nil
	case ">":
		return func(record executor.Record) bool {
			return string(record[pos]) > value
		}, nil
	case ">=":
		return func(record executor.Record) bool {
			return string(record[pos]) >= value
		}, nil
	default:
		return nil, fmt.Errorf("unsupported operator in WHERE clause: %s", operator)
	}
}
