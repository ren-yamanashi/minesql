package parser

import (
	"errors"
	"minesql/internal/ast"
	"strings"
)

// WhereParser は WHERE 句のパース処理を共通化する構造体。
// SelectParser, DeleteParser などから利用する。
type WhereParser struct {
	// 現在構築中の WHERE 句
	whereClause *ast.WhereClause
	// WHERE 句の AST ノードが格納されるスタック
	nodeStack []ast.ASTNode
	// WHERE 句の演算子が格納されるスタック
	opStack []string
}

// WHERE 句のパースを開始する (WHERE キーワードを検出した時点で呼ぶ)
func (wp *WhereParser) initWhere() *ast.WhereClause {
	wp.whereClause = &ast.WhereClause{}
	wp.nodeStack = []ast.ASTNode{}
	wp.opStack = []string{}
	return wp.whereClause
}

// 識別子をカラム名としてスタックに積む
func (wp *WhereParser) pushColumn(ident string) {
	colId := *ast.NewColumnId(ident)
	wp.nodeStack = append(wp.nodeStack, colId)
}

// リテラルをスタックに積む
func (wp *WhereParser) pushLiteral(lit ast.Literal) {
	wp.nodeStack = append(wp.nodeStack, lit)
}

// 演算子を処理する (Shunting Yard アルゴリズム)
func (wp *WhereParser) handleOperator(op string) error {
	// 新しい演算子を積む前に、スタックにある「優先順位が高い or 同じ」演算子を処理する
	for len(wp.opStack) > 0 {
		topOp := wp.opStack[len(wp.opStack)-1]
		if wp.precedence(topOp) >= wp.precedence(op) {
			if err := wp.reduce(); err != nil {
				return err
			}
		} else {
			break
		}
	}
	wp.opStack = append(wp.opStack, op)
	return nil
}

// WHERE 句を確定する (finalize 時に呼ぶ)。
// WHERE 句が未設定の場合は nil を返す。
func (wp *WhereParser) finalizeWhere() (*ast.WhereClause, error) {
	// WHERE 句がない場合は nil を返す
	if wp.whereClause == nil {
		return nil, nil
	}

	// 残っている演算子をすべて処理
	for len(wp.opStack) > 0 {
		if err := wp.reduce(); err != nil {
			return nil, err
		}
	}

	// WHERE 句があるのに式が一つもない場合はエラー
	if len(wp.nodeStack) == 0 {
		return nil, errors.New("[parse error] empty expression in WHERE clause")
	}

	// スタックに複数の要素が残っている場合はエラー
	if len(wp.nodeStack) != 1 {
		return nil, errors.New("[parse error] incomplete expression in WHERE clause")
	}

	// 最後に残った式を、WHERE 句のルートの式として設定
	finalExpr, ok := wp.nodeStack[0].(*ast.BinaryExpr)
	if !ok {
		return nil, errors.New("[parse error] invalid expression result")
	}
	wp.whereClause.Condition = finalExpr

	return wp.whereClause, nil
}

// スタックから要素を取り出し、1 つの BinaryExpr を作って nodeStack に戻す
// e.g.
// - nodeStack: [name, "john"], opStack: ["="] -> nodeStack: [BinaryExpr(name = "john")]
// - nodeStack: [age, 30, BinaryExpr(name = "john")], opStack: [">", "AND"] -> nodeStack: [BinaryExpr(age > 30 AND name = "john")]
func (wp *WhereParser) reduce() error {
	if len(wp.nodeStack) < 2 || len(wp.opStack) < 1 {
		return errors.New("[parse error] invalid expression syntax")
	}

	// 右辺を Pop (スタックは LIFO なので先に右辺が出てくる)
	rightRaw := wp.nodeStack[len(wp.nodeStack)-1]
	wp.nodeStack = wp.nodeStack[:len(wp.nodeStack)-1]

	// 演算子を Pop
	op := wp.opStack[len(wp.opStack)-1]
	wp.opStack = wp.opStack[:len(wp.opStack)-1]

	// 左辺を Pop
	leftRaw := wp.nodeStack[len(wp.nodeStack)-1]
	wp.nodeStack = wp.nodeStack[:len(wp.nodeStack)-1]

	var lhs ast.LHS
	var rhs ast.RHS

	// 左辺の型判定
	switch v := leftRaw.(type) {
	case ast.ColumnId:
		lhs = ast.NewLhsColumn(v)
	case ast.Expression:
		lhs = ast.NewLhsExpr(v)
	default:
		return errors.New("[parse error] invalid left operand type")
	}

	// 右辺の型判定
	switch v := rightRaw.(type) {
	case ast.Literal:
		rhs = ast.NewRhsLiteral(v)
	case ast.Expression:
		rhs = ast.NewRhsExpr(v)
	default:
		return errors.New("[parse error] invalid right operand type")
	}

	// BinaryExpr を作成してスタックに積む (これが次の演算の左辺や右辺になる)
	expr := ast.NewBinaryExpr(op, lhs, rhs)
	wp.nodeStack = append(wp.nodeStack, expr)

	return nil
}

// 演算子の優先順位を定義 (数値が高いほど優先順位が高い)
func (wp *WhereParser) precedence(op string) int {
	switch strings.ToUpper(op) {
	case string(SEqual), string(SLessThan), string(SGreaterThan), "<=", ">=", "!=":
		return 2 // 比較演算子
	case KAnd:
		return 1 // 論理演算子
	case KOr:
		return 0 // 論理演算子
	default:
		return 0
	}
}
