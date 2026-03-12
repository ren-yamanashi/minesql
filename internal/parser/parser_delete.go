package parser

import (
	"errors"
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/literal"
	"minesql/internal/planner/ast/node"
	"minesql/internal/planner/ast/statement"
	"strings"
)

var (
	ErrDeleteStmtIsNil error = errors.New("[internal error] DeleteStmt is nil")
)

type DeleteParser struct {
	// 現在のステート
	state ParserState
	// 現在構築中の DELETE 文
	stmt *statement.DeleteStmt
	// 現在構築中の WHERE 句
	whereClause *statement.WhereClause
	// WHERE 句の AST ノードが格納されるスタック
	whereNodeStack []node.ASTNode
	// WHERE 句の演算子が格納されるスタック
	whereOpStack []string
	// エラー情報
	err error
}

func NewDeleteParser() *DeleteParser {
	return &DeleteParser{
		state: DeleteStateDelete,
	}
}

func (dp *DeleteParser) getResult() node.ASTNode {
	return dp.stmt
}

func (dp *DeleteParser) getError() error {
	return dp.err
}

func (dp *DeleteParser) finalize() {
	if dp.err != nil {
		return
	}

	// DELETE 文がない場合はエラー
	if dp.stmt == nil {
		dp.setError(errors.New("[parse error] must have DELETE statement"))
		return
	}

	// テーブル名が空の場合はエラー (FROM 句がない場合を含む)
	if dp.stmt.From.TableName == "" {
		dp.setError(errors.New("[parse error] missing FROM clause"))
		return
	}

	// ステートが End でない場合はエラー
	if dp.state != DeleteStateEnd {
		dp.setError(errors.New("[parse error] incomplete DELETE statement"))
		return
	}

	// WHERE 句がない場合は空の WhereClause を設定
	if dp.whereClause == nil {
		dp.stmt.Where = &statement.WhereClause{IsSet: false}
		return
	}

	// 残っている演算子をすべて処理
	for len(dp.whereOpStack) > 0 {
		if err := dp.reduce(); err != nil {
			dp.setError(err)
			return
		}
	}

	// WHERE 句があるのに式が一つもない場合はエラー
	if len(dp.whereNodeStack) == 0 {
		dp.setError(errors.New("[parse error] empty expression in WHERE clause"))
		return
	}

	// スタックに複数の要素が残っている場合はエラー
	if len(dp.whereNodeStack) != 1 {
		dp.setError(errors.New("[parse error] incomplete expression in WHERE clause"))
		return
	}

	// 最後に残った式を、WHERE 句のルートの式として設定
	finalExpr, ok := dp.whereNodeStack[0].(expression.Expression)
	if !ok {
		dp.setError(errors.New("[parse error] invalid expression result"))
		return
	}
	dp.whereClause.Condition = finalExpr
}

func (dp *DeleteParser) OnKeyword(word string) {
	if dp.err != nil {
		return
	}
	upperWord := strings.ToUpper(word)

	switch upperWord {
	case KDelete:
		dp.stmt = &statement.DeleteStmt{StmtType: statement.StmtTypeDelete}
		dp.state = DeleteStateDelete
		return

	case KFrom:
		if dp.state == DeleteStateDelete {
			dp.state = DeleteStateFrom
			return
		}
		dp.setError(errors.New("[parse error] FROM clause is in invalid position"))
		return

	case KWhere:
		if dp.state == DeleteStateFrom {
			dp.whereClause = &statement.WhereClause{IsSet: true}
			dp.stmt.Where = dp.whereClause
			dp.whereNodeStack = []node.ASTNode{}
			dp.whereOpStack = []string{}
			dp.state = DeleteStateWhere
			return
		}
		dp.setError(errors.New("[parse error] WHERE clause is in invalid position"))
		return

	case KAnd, KOr:
		if dp.state == DeleteStateWhere {
			dp.handleOperator(upperWord)
			return
		}
		dp.setError(errors.New("[parse error] AND operator is in invalid position"))
		return

	default:
		dp.setError(errors.New("[parse error] unsupported keyword: " + word))
		return
	}
}

func (dp *DeleteParser) OnIdentifier(ident string) {
	if dp.err != nil {
		return
	}

	switch dp.state {
	case DeleteStateFrom:
		dp.stmt.From = *identifier.NewTableId(ident)
	case DeleteStateWhere:
		colId := *identifier.NewColumnId(ident)
		dp.whereNodeStack = append(dp.whereNodeStack, colId)
	default:
		dp.setError(errors.New("[parse error] unexpected identifier: " + ident))
	}
}

func (dp *DeleteParser) OnSymbol(symbol string) {
	if dp.err != nil {
		return
	}

	// ";" が来たら state を End にする
	if symbol == string(SSemicolon) {
		dp.state = DeleteStateEnd
		return
	}

	switch dp.state {
	case DeleteStateWhere:
		dp.handleOperator(symbol)
	default:
		dp.setError(errors.New("[parse error] unexpected symbol: " + symbol))
	}
}

func (dp *DeleteParser) OnString(value string) {
	dp.handleLiteral(literal.NewStringLiteral(value, value))
}

func (dp *DeleteParser) OnNumber(num string) {
	dp.handleLiteral(literal.NewStringLiteral(num, num))
}

func (dp *DeleteParser) OnComment(text string) {
	// 何もしない
}

func (dp *DeleteParser) OnError(err error) {
	dp.setError(err)
}

// リテラルを処理する
func (dp *DeleteParser) handleLiteral(lit literal.Literal) {
	if dp.err != nil {
		return
	}
	if dp.state == DeleteStateWhere {
		dp.whereNodeStack = append(dp.whereNodeStack, lit)
	}
}

// 演算子を処理する
func (dp *DeleteParser) handleOperator(op string) {
	for len(dp.whereOpStack) > 0 {
		topOp := dp.whereOpStack[len(dp.whereOpStack)-1]
		if dp.precedence(topOp) >= dp.precedence(op) {
			if err := dp.reduce(); err != nil {
				dp.setError(err)
				return
			}
		} else {
			break
		}
	}
	dp.whereOpStack = append(dp.whereOpStack, op)
}

// スタックから要素を取り出し、1 つの BinaryExpr を作って nodeStack に戻す
func (dp *DeleteParser) reduce() error {
	if len(dp.whereNodeStack) < 2 || len(dp.whereOpStack) < 1 {
		return errors.New("[parse error] invalid expression syntax")
	}

	// 右辺を Pop
	rightRaw := dp.whereNodeStack[len(dp.whereNodeStack)-1]
	dp.whereNodeStack = dp.whereNodeStack[:len(dp.whereNodeStack)-1]

	// 演算子を Pop
	op := dp.whereOpStack[len(dp.whereOpStack)-1]
	dp.whereOpStack = dp.whereOpStack[:len(dp.whereOpStack)-1]

	// 左辺を Pop
	leftRaw := dp.whereNodeStack[len(dp.whereNodeStack)-1]
	dp.whereNodeStack = dp.whereNodeStack[:len(dp.whereNodeStack)-1]

	var lhs expression.LHS
	var rhs expression.RHS

	switch v := leftRaw.(type) {
	case identifier.ColumnId:
		lhs = expression.NewLhsColumn(v)
	case expression.Expression:
		lhs = expression.NewLhsExpr(v)
	default:
		return errors.New("[parse error] invalid left operand type")
	}

	switch v := rightRaw.(type) {
	case literal.Literal:
		rhs = expression.NewRhsLiteral(v)
	case expression.Expression:
		rhs = expression.NewRhsExpr(v)
	default:
		return errors.New("[parse error] invalid right operand type")
	}

	expr := expression.NewBinaryExpr(op, lhs, rhs)
	dp.whereNodeStack = append(dp.whereNodeStack, expr)

	return nil
}

// 演算子の優先順位を定義 (数値が高いほど優先順位が高い)
func (dp *DeleteParser) precedence(op string) int {
	switch strings.ToUpper(op) {
	case string(SEqual), string(SLessThan), string(SGreaterThan), "<=", ">=", "!=":
		return 2
	case KAnd:
		return 1
	case KOr:
		return 0
	default:
		return 0
	}
}

// エラーを設定する (既にエラーが設定されている場合は無視する)
func (dp *DeleteParser) setError(err error) {
	if dp.err == nil {
		dp.err = err
	}
}
