package planner

import (
	"errors"
	"fmt"
	"minesql/internal/executor"
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/statement"
	"minesql/internal/storage"
	"minesql/internal/storage/catalog"
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

// 二項演算式を解析して適切な検索用の Executor を構築する
func planForBinaryExpr(tableName string, tblMeta *catalog.TableMetadata, expr expression.BinaryExpr) (executor.Executor, error) {
	switch lhs := expr.Left.(type) {

	// 左辺がカラムの場合 (例: WHERE col = 5)
	case *expression.LhsColumn:
		colName := lhs.Column.ColName
		switch rhs := expr.Right.(type) {
		// 左辺がカラムの場合、右辺はリテラルでなければならない (例: WHERE col = 5)
		case *expression.RhsLiteral:
			if _, ok := tblMeta.GetColByName(colName); !ok {
				return nil, errors.New("column " + colName + " does not exist in table " + tableName)
			}

			// カラムがインデックスの場合 (該当のカラムをキーとしたインデックスが存在する場合)、インデックス検索を行う
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

			// カラムがインデックスでない場合、テーブル検索を行う
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

	// 左辺が式の場合 (例: WHERE col1 = 5 AND col2 > 10)
	case *expression.LhsExpr:
		// 式の木構造から単一の条件関数を再帰的に構築する
		cond, err := buildConditionFunc(tableName, tblMeta, expr)
		if err != nil {
			return nil, err
		}

		// 全件スキャン -> 条件の適用 の流れで実行する (フィルタリング用の executor の innerExecutor としてテーブルスキャン用の executor を渡す)
		return executor.NewFilter(
			executor.NewSearchTable( // innerExecutor としてテーブルスキャン用の executor を渡す
				tableName,
				executor.RecordSearchModeStart{},
				func(record executor.Record) bool {
					return true
				},
			),
			cond,
		), nil

	default:
		return nil, errors.New("unsupported LHS type in binary expression")
	}
}

// 式の木構造から単一の条件関数を再帰的に構築する
func buildConditionFunc(tableName string, tblMeta *catalog.TableMetadata, expr expression.BinaryExpr) (func(executor.Record) bool, error) {
	switch lhs := expr.Left.(type) {

	// リーフノード: col of literal のような単純な条件 (例: col1 = 5)
	case *expression.LhsColumn:
		colName := lhs.Column.ColName
		colMeta, ok := tblMeta.GetColByName(colName)
		if !ok {
			return nil, errors.New("column " + colName + " does not exist in table " + tableName)
		}

		switch rhs := expr.Right.(type) {
		// 左辺がカラムで右辺がリテラルの場合 (例: col1 = 5)
		case *expression.RhsLiteral:
			return operatorToCondition(expr.Operator, int(colMeta.Pos), rhs.Literal.ToString())
		// 左辺がカラムの場合は右辺はリテラルでなければならない (`col1 = col2` のような条件は現状サポートしていない)
		default:
			return nil, errors.New("when LHS is a column, RHS must be a literal")
		}

	// ブランチノード: expr AND/OR expr (例: col1 = 5 AND col2 > 10 のような複合条件)
	case *expression.LhsExpr:
		// 左辺の式から条件関数を再帰的に構築
		leftCond, err := buildConditionFunc(tableName, tblMeta, *lhs.Expr.(*expression.BinaryExpr))
		if err != nil {
			return nil, err
		}

		switch rhs := expr.Right.(type) {
		// 右辺が式の場合、右辺の式から条件関数を再帰的に構築し、論理演算子 (AND/OR) に応じて条件関数を組み合わせる
		case *expression.RhsExpr:
			rightCond, err := buildConditionFunc(tableName, tblMeta, *rhs.Expr.(*expression.BinaryExpr))
			if err != nil {
				return nil, err
			}
			switch expr.Operator {
			case "AND":
				return func(r executor.Record) bool { return leftCond(r) && rightCond(r) }, nil
			case "OR":
				return func(r executor.Record) bool { return leftCond(r) || rightCond(r) }, nil
			default:
				return nil, fmt.Errorf("unsupported logical operator: %s", expr.Operator)
			}
		// 左辺が式の場合は右辺も式でなければならない
		default:
			return nil, errors.New("when LHS is an expression, RHS cannot be a literal")
		}

	default:
		panic("unsupported LHS type in buildConditionFunc")
	}
}

// 二項演算子を条件関数に変換する
// 条件関数: レコードを受け取り、条件を満たすかどうか (bool) を返す関数
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
