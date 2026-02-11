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
	ErrSelectStmtIsNil  error = errors.New("[internal error] SelectStmt is nil")
	ErrWhereClauseIsNil error = errors.New("[internal error] WhereClause is nil")
)

type SelectParser struct {
	// 現在のステート
	state ParserState
	// 現在構築中の SELECT 文
	stmt *statement.SelectStmt
	// 現在構築中の WHERE 句
	whereClause *statement.WhereClause
	// WHERE 句の AST ノードが格納されるスタック
	whereNodeStack []node.ASTNode
	// WHERE 句の演算子が格納されるスタック
	whereOpStack []string
	// エラー情報
	err error
}

func NewSelectParser() *SelectParser {
	return &SelectParser{
		state: SelectStateColumns,
	}
}

func (sp *SelectParser) getResult() node.ASTNode {
	return sp.stmt
}

func (sp *SelectParser) getError() error {
	return sp.err
}

func (sp *SelectParser) finalize() {
	if sp.err != nil {
		return
	}

	// SELECT 文がない場合はエラー
	if sp.stmt == nil {
		sp.setError(errors.New("[parse error] must have SELECT statement"))
		return
	}

	// テーブル名が空の場合はエラー (FROM 句がない場合を含む)
	if sp.stmt.From.TableName == "" {
		sp.setError(errors.New("[parse error] missing FROM clause"))
		return
	}

	// ステートが End でない場合はエラー
	if sp.state != SelectStateEnd {
		sp.setError(errors.New("[parse error] incomplete SELECT statement"))
		return
	}

	// WHERE 句がない場合は空の WhereClause を設定
	if sp.whereClause == nil {
		sp.stmt.Where = &statement.WhereClause{IsSet: false}
		return
	}

	// 残っている演算子をすべて処理
	for len(sp.whereOpStack) > 0 {
		if err := sp.reduce(); err != nil {
			sp.setError(err)
			return
		}
	}

	// WHERE 句があるのに式が一つもない場合はエラー
	if len(sp.whereNodeStack) == 0 {
		sp.setError(errors.New("[parse error] empty expression in WHERE clause"))
		return
	}

	// スタックに複数の要素が残っている場合はエラー
	if len(sp.whereNodeStack) != 1 {
		sp.setError(errors.New("[parse error] incomplete expression in WHERE clause"))
		return
	}

	// 最後に残った式を、WHERE 句のルートの式として設定
	finalExpr, ok := sp.whereNodeStack[0].(expression.Expression)
	if !ok {
		sp.setError(errors.New("[parse error] invalid expression result"))
		return
	}
	sp.whereClause.Condition = finalExpr
}

func (sp *SelectParser) OnKeyword(word string) {
	if sp.err != nil {
		return
	}
	upperWord := strings.ToUpper(word)

	switch upperWord {
	case KSelect:
		sp.stmt = &statement.SelectStmt{StmtType: statement.StmtTypeSelect}
		sp.state = SelectStateColumns
		return

	case KFrom:
		if sp.state == SelectStateColumns {
			sp.state = SelectStateFrom
			return
		}
		sp.setError(errors.New("[parse error] FROM clause is in invalid position"))
		return

	case KWhere:
		if sp.state == SelectStateFrom {
			sp.whereClause = &statement.WhereClause{IsSet: true}
			sp.stmt.Where = sp.whereClause
			sp.whereNodeStack = []node.ASTNode{}
			sp.whereOpStack = []string{}
			sp.state = SelectStateWhere
			return
		}
		sp.setError(errors.New("[parse error] WHERE clause is in invalid position"))
		return

	case KAnd, KOr:
		if sp.state == SelectStateWhere {
			sp.handleOperator(upperWord)
			return
		}
		sp.setError(errors.New("[parse error] AND operator is in invalid position"))
		return

	default:
		sp.setError(errors.New("[parse error] unsupported keyword: " + word))
		return
	}
}

func (sp *SelectParser) OnIdentifier(ident string) {
	if sp.err != nil {
		return
	}

	switch sp.state {
	case SelectStateColumns:
		// 現状 SELECT 句では "*" のみサポートしているので、Identifier が来たらエラー
		sp.setError(errors.New("[parse error] currently only SELECT * is supported"))
		return
	case SelectStateFrom:
		sp.stmt.From = *identifier.NewTableId(ident)
	case SelectStateWhere:
		// WHERE 句で扱う Identifier はカラム名のみのため、ColumnId として扱い、スタックに積む
		colId := *identifier.NewColumnId(ident)
		sp.whereNodeStack = append(sp.whereNodeStack, colId)
	}
}

func (sp *SelectParser) OnSymbol(symbol string) {
	if sp.err != nil {
		return
	}

	// ";" が来たら state を End にする
	if symbol == string(SSemicolon) {
		sp.state = SelectStateEnd
		return
	}

	switch sp.state {
	case SelectStateColumns:
		// 現状 SELECT 句では "*" のみサポートしているので、"*" 以外のシンボルが来たらエラー
		if symbol != string(CAsterisk) {
			sp.setError(errors.New("[parse error] currently only SELECT * is supported"))
			return
		}
	case SelectStateFrom:
		// FROM 句ではシンボルは来ないはずなのでエラー
		sp.setError(errors.New("[parse error] unexpected symbol in FROM clause: " + symbol))
		return
	case SelectStateWhere:
		sp.handleOperator(symbol)
	}
}

func (sp *SelectParser) OnString(value string) {
	sp.handleLiteral(literal.NewStringLiteral(value, value))
}

func (sp *SelectParser) OnNumber(num string) {
	sp.handleLiteral(literal.NewStringLiteral(num, num))
}

func (sp *SelectParser) OnComment(text string) {
	// 何もしない
}

func (sp *SelectParser) OnError(err error) {
	sp.setError(err)
}

// リテラルを処理する
func (sp *SelectParser) handleLiteral(lit literal.Literal) {
	if sp.err != nil {
		return
	}
	if sp.state == SelectStateWhere {
		sp.whereNodeStack = append(sp.whereNodeStack, lit)
	}
}

// 演算子を処理する
func (sp *SelectParser) handleOperator(op string) {
	// 新しい演算子を積む前に、スタックにある「優先順位が高い or 同じ」演算子を処理する
	// e.g. スタックに "=" があって、今 "AND" が来た場合 -> 先に "=" を reduce する
	for len(sp.whereOpStack) > 0 {
		topOp := sp.whereOpStack[len(sp.whereOpStack)-1]
		if sp.precedence(topOp) >= sp.precedence(op) {
			if err := sp.reduce(); err != nil {
				sp.setError(err)
				return
			}
		} else {
			break
		}
	}
	sp.whereOpStack = append(sp.whereOpStack, op)
}

// スタックから要素を取り出し、1つの BinaryExpr を作って nodeStack に戻す
// e.g.
// - nodeStack: [name, "john"], opStack: ["="] -> nodeStack: [BinaryExpr(name = "john")]
// - nodeStack: [age, 30, BinaryExpr(name = "john")], opStack: [">", "AND"] -> nodeStack: [BinaryExpr(age > 30 AND name = "john")]
func (sp *SelectParser) reduce() error {
	// 必要な要素が足りているかチェック
	if len(sp.whereNodeStack) < 2 || len(sp.whereOpStack) < 1 {
		return errors.New("[parse error] invalid expression syntax")
	}

	// 右辺を Pop (スタックはLIFOなので先に右辺が出てくる)
	rightRaw := sp.whereNodeStack[len(sp.whereNodeStack)-1]
	sp.whereNodeStack = sp.whereNodeStack[:len(sp.whereNodeStack)-1]

	// 演算子を Pop
	op := sp.whereOpStack[len(sp.whereOpStack)-1]
	sp.whereOpStack = sp.whereOpStack[:len(sp.whereOpStack)-1]

	// 左辺を Pop
	leftRaw := sp.whereNodeStack[len(sp.whereNodeStack)-1]
	sp.whereNodeStack = sp.whereNodeStack[:len(sp.whereNodeStack)-1]

	//
	// --- 左辺・右辺の型判定と BinaryExpr 作成 ---
	//

	var lhs expression.LHS
	var rhs expression.RHS

	// 左辺の型判定
	switch v := leftRaw.(type) {
	case identifier.ColumnId:
		lhs = expression.NewLhsColumn(v)
	case expression.Expression:
		lhs = expression.NewLhsExpr(v)
	default:
		return errors.New("[parse error] invalid left operand type")
	}

	// 右辺の型判定
	switch v := rightRaw.(type) {
	case literal.Literal:
		rhs = expression.NewRhsLiteral(v)
	case expression.Expression:
		rhs = expression.NewRhsExpr(v)
	default:
		return errors.New("[parse error] invalid right operand type")
	}

	// BinaryExpr を作成してスタックに積む (これが次の演算の左辺や右辺になる)
	expr := expression.NewBinaryExpr(op, lhs, rhs)
	sp.whereNodeStack = append(sp.whereNodeStack, expr)

	return nil
}

// 演算子の優先順位を定義 (数値が高いほど優先順位が高い)
func (sp *SelectParser) precedence(op string) int {
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

// エラーを設定する (既にエラーが設定されている場合は無視する)
func (sp *SelectParser) setError(err error) {
	if sp.err == nil {
		sp.err = err
	}
}
