package planner

import (
	"errors"
	"fmt"

	"minesql/internal/ast"
	"minesql/internal/executor"
)

// -------------------------------------------------
// リーフ条件の抽出
// -------------------------------------------------

// extractANDLeaves は純粋な AND ツリーからリーフ条件を抽出する
//
// OR が含まれている場合は nil を返す
func extractANDLeaves(expr ast.BinaryExpr) []leafCondition {
	// リーフ: LhsColumn op RhsLiteral
	if lhs, ok := expr.Left.(*ast.LhsColumn); ok {
		if rhs, ok := expr.Right.(*ast.RhsLiteral); ok {
			return []leafCondition{{
				colName:  lhs.Column.ColName,
				operator: expr.Operator,
				literal:  rhs.Literal,
			}}
		}
		return nil
	}

	// ブランチ: LhsExpr AND/OR RhsExpr
	lhsExpr, lhsOk := expr.Left.(*ast.LhsExpr)
	rhsExpr, rhsOk := expr.Right.(*ast.RhsExpr)
	if !lhsOk || !rhsOk {
		return nil
	}

	// OR が含まれていたら最適化不可
	if expr.Operator != "AND" {
		return nil
	}

	leftLeaves := extractANDLeaves(*lhsExpr.Expr)
	if leftLeaves == nil {
		return nil
	}
	rightLeaves := extractANDLeaves(*rhsExpr.Expr)
	if rightLeaves == nil {
		return nil
	}

	return append(leftLeaves, rightLeaves...)
}

// extractORBranches は OR ツリーから各ブランチを抽出する
//
// 各ブランチは単一条件 (col op literal) または複合 AND 条件を保持できる
// AND サブツリーは extractANDLeaves でリーフ条件に分解する
// 分解できないブランチがある場合は nil を返す
func extractORBranches(expr ast.BinaryExpr) []orBranch {
	// リーフ: LhsColumn op RhsLiteral → 単一条件のブランチ
	if lhs, ok := expr.Left.(*ast.LhsColumn); ok {
		if rhs, ok := expr.Right.(*ast.RhsLiteral); ok {
			leaf := leafCondition{
				colName:  lhs.Column.ColName,
				operator: expr.Operator,
				literal:  rhs.Literal,
			}
			return []orBranch{{leaves: []leafCondition{leaf}, expr: expr}}
		}
		return nil
	}

	// ブランチ: LhsExpr op RhsExpr
	lhsExpr, lhsOk := expr.Left.(*ast.LhsExpr)
	rhsExpr, rhsOk := expr.Right.(*ast.RhsExpr)
	if !lhsOk || !rhsOk {
		return nil
	}

	if expr.Operator == "OR" {
		// OR ノード: 左右を再帰して連結
		leftBranches := extractORBranches(*lhsExpr.Expr)
		if leftBranches == nil {
			return nil
		}
		rightBranches := extractORBranches(*rhsExpr.Expr)
		if rightBranches == nil {
			return nil
		}
		return append(leftBranches, rightBranches...)
	}

	// AND ノード (またはその他): サブツリー全体を 1 つのブランチとして扱う
	leaves := extractANDLeaves(expr)
	if leaves == nil {
		return nil
	}
	return []orBranch{{leaves: leaves, expr: expr}}
}

// -------------------------------------------------
// 条件関数の構築
// -------------------------------------------------

// buildConditionFunc は式の木構造から単一の条件関数を再帰的に構築する
func (s *Search) buildConditionFunc(expr ast.BinaryExpr) (func(executor.Record) bool, error) {
	switch lhs := expr.Left.(type) {

	// リーフノード: col op literal のような単純な条件 (例: col1 = 5)
	case *ast.LhsColumn:
		colName := lhs.Column.ColName
		colMeta, ok := s.tblMeta.GetColByName(colName)
		if !ok {
			return nil, errors.New("column " + colName + " does not exist in table " + s.tblMeta.Name)
		}

		switch rhs := expr.Right.(type) {
		// 左辺がカラムで右辺がリテラルの場合 (例: col1 = 5)
		case *ast.RhsLiteral:
			return s.operatorToCondition(expr.Operator, int(colMeta.Pos), rhs.Literal.ToString())
		// 左辺がカラムの場合は右辺はリテラルでなければならない (`col1 = col2` のような条件は現状サポートしていない)
		default:
			return nil, errors.New("when LHS is a column, RHS must be a literal")
		}

	// ブランチノード: expr AND/OR expr (例: col1 = 5 AND col2 > 10 のような複合条件)
	case *ast.LhsExpr:
		// 左辺の式から条件関数を再帰的に構築
		leftCond, err := s.buildConditionFunc(*lhs.Expr)
		if err != nil {
			return nil, err
		}

		switch rhs := expr.Right.(type) {
		// 右辺が式の場合、右辺の式から条件関数を再帰的に構築し、論理演算子 (AND/OR) に応じて条件関数を組み合わせる
		case *ast.RhsExpr:
			rightCond, err := s.buildConditionFunc(*rhs.Expr)
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

// operatorToCondition は二項演算子を条件関数に変換する
//
// 条件関数: レコードを受け取り、条件を満たすかどうか (bool) を返す関数
func (s *Search) operatorToCondition(operator string, pos int, value string) (func(executor.Record) bool, error) {
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
