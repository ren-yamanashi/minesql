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

// SearchPlanner は WHERE 句に基づいてレコードを検索する Executor を構築する
type SearchPlanner struct {
	TableName string
	Where     *statement.WhereClause
}

func NewSearchPlanner(tableName string, where *statement.WhereClause) *SearchPlanner {
	return &SearchPlanner{
		TableName: tableName,
		Where:     where,
	}
}

func (sp *SearchPlanner) Next() (executor.Executor, error) {
	return buildSearchExecutor(sp.TableName, sp.Where)
}

// WHERE 句を元に検索用の Executor を構築する
func buildSearchExecutor(tableName string, where *statement.WhereClause) (executor.Executor, error) {
	sm := storage.GetStorageManager()

	if tableName == "" {
		return nil, errors.New("table name cannot be empty")
	}

	tblMeta, err := sm.Catalog.GetTableMetadataByName(tableName)
	if err != nil {
		return nil, err
	}

	// WHERE 句が設定されていない場合フルテーブルスキャンを実行
	if where == nil || !where.IsSet {
		return executor.NewSearchTable(
			tableName,
			executor.RecordSearchModeStart{},
			func(record executor.Record) bool {
				return true // フルテーブルスキャンなので常に true を返す
			},
		), nil
	}

	// WHERE 句が設定されている場合
	switch e := where.Condition.(type) {
	case *expression.BinaryExpr:
		return planForBinaryExpr(tableName, tblMeta, *e)
	default:
		return nil, errors.New("unsupported WHERE condition type")
	}
}

func planForBinaryExpr(tableName string, tblMeta *catalog.TableMetadata, expr expression.BinaryExpr) (executor.Executor, error) {
	switch lhs := expr.Left.(type) {
	case *expression.LhsColumn:
		colName := lhs.Column.ColName
		switch rhs := expr.Right.(type) {
		case *expression.RhsLiteral:
			if _, ok := tblMeta.GetColByName(colName); !ok {
				return nil, errors.New("column " + colName + " does not exist in table " + tableName)
			}
			// カラムがインデックスの場合
			if idxMeta, ok := tblMeta.GetIndexByColName(colName); ok {
				cond, err := operatorToCondition(expr.Operator, 0, rhs.Literal.ToString())
				if err != nil {
					return nil, err
				}
				return executor.NewSearchIndex(
					tableName,
					idxMeta.Name,
					executor.RecordSearchModeKey{Key: [][]byte{rhs.Literal.ToBytes()}},
					cond,
				), nil
			}
			// カラムがインデックスでない場合
			colMeta, ok := tblMeta.GetColByName(colName)
			if !ok {
				return nil, errors.New("column " + colName + " does not exist in table " + tableName)
			}
			cond, err := operatorToCondition(expr.Operator, int(colMeta.Pos), rhs.Literal.ToString())
			if err != nil {
				return nil, err
			}
			return executor.NewSearchTable(
				tableName,
				executor.RecordSearchModeKey{Key: [][]byte{rhs.Literal.ToBytes()}},
				cond,
			), nil
		default:
			return nil, errors.New("when LHS is a column, RHS must be a literal")
		}
	case *expression.LhsExpr:
		if expr.Operator != "AND" {
			return nil, errors.New("only AND operator is supported for combining multiple conditions")
		}
		conditions, err := extractConditions(tableName, tblMeta, expr)
		if err != nil {
			return nil, err
		}
		seqScan := executor.NewSearchTable(
			tableName,
			executor.RecordSearchModeStart{},
			func(record executor.Record) bool {
				return true
			},
		)
		return executor.NewFilter(
			seqScan,
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
func extractConditions(tableName string, tblMeta *catalog.TableMetadata, expr expression.BinaryExpr) ([]func(executor.Record) bool, error) {
	conditions := []func(executor.Record) bool{}

	switch lhs := expr.Left.(type) {
	case *expression.LhsColumn:
		colName := lhs.Column.ColName
		colMeta, ok := tblMeta.GetColByName(colName)
		if !ok {
			return nil, errors.New("column " + colName + " does not exist in table " + tableName)
		}

		switch rhs := expr.Right.(type) {
		case *expression.RhsLiteral:
			cond, err := operatorToCondition(expr.Operator, int(colMeta.Pos), rhs.Literal.ToString())
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, cond)
		case *expression.RhsExpr:
			return nil, errors.New("when LHS is a column, RHS must be a literal")
		default:
			panic("unsupported RHS type in extractConditions") // 実際にはここには到達しないので errors.New ではなく panic で良い
		}

	case *expression.LhsExpr:
		if expr.Operator != "AND" {
			return nil, errors.New("only AND operator is supported for combining multiple conditions")
		}
		leftConds, err := extractConditions(tableName, tblMeta, *lhs.Expr.(*expression.BinaryExpr))
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, leftConds...)

		switch rhs := expr.Right.(type) {
		case *expression.RhsExpr:
			rightConds, err := extractConditions(tableName, tblMeta, *rhs.Expr.(*expression.BinaryExpr))
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, rightConds...)
		case *expression.RhsLiteral:
			return nil, errors.New("when LHS is an expression, RHS cannot be a literal")
		default:
			panic("unsupported RHS type in extractConditions") // 実際にはここには到達しないので errors.New ではなく panic で良い
		}
	default:
		panic("unsupported LHS type in extractConditions") // 実際にはここには到達しないので errors.New ではなく panic で良い
	}

	return conditions, nil
}

func operatorToCondition(operator string, pos int, value string) (func(executor.Record) bool, error) {
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
