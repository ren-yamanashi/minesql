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

const (
	StateSelectColumns ParserState = 100 + iota // SELECT
	StateFrom                                   // FROM
	StateWhere                                  // WHERE
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
	// AST ノードが格納されるスタック (Where 句の式構築用)
	nodeStack []node.ASTNode
	// 演算子が格納されるスタック (Where 句の式構築用)
	opStack []string
	// エラー情報
	err error
}

func NewSelectParser() *SelectParser {
	return &SelectParser{
		state: StateSelectColumns,
	}
}

func (sp *SelectParser) getResult() node.ASTNode {
	return sp.stmt
}

func (sp *SelectParser) getError() error {
	return sp.err
}

func (sp *SelectParser) finalize() {
	// SELECT 文がない場合はエラー
	if sp.stmt == nil {
		sp.setError(errors.New("[parse error] must have SELECT statement"))
		return
	}

	// FROM 句がない場合はエラー
	if sp.state == StateSelectColumns {
		sp.setError(errors.New("[parse error] missing FROM clause"))
		return
	}

	// WHERE 句がない場合は何もしない
	if sp.whereClause == nil {
		return
	}

	// 残っている演算子をすべて処理
	for len(sp.opStack) > 0 {
		if err := sp.reduce(); err != nil {
			sp.setError(err)
			return
		}
	}

	// WHERE 句があるのに式が一つもない場合はエラー
	if len(sp.nodeStack) == 0 {
		sp.setError(errors.New("[parse error] empty expression in WHERE clause"))
		return
	}

	// スタックに複数の要素が残っている場合はエラー
	if len(sp.nodeStack) != 1 {
		sp.setError(errors.New("[parse error] incomplete expression in WHERE clause"))
		return
	}

	// 最後に残った1つが、完成したルートの Expression
	finalExpr, ok := sp.nodeStack[0].(expression.Expression)
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
	case "SELECT":
		sp.stmt = &statement.SelectStmt{StmtType: statement.StmtTypeSelect}
		sp.state = StateSelectColumns
		return

	case "FROM":
		if sp.state == StateSelectColumns {
			sp.state = StateFrom
			return
		}
		sp.setError(errors.New("[parse error] FROM clause is in invalid position"))
		return

	case "WHERE":
		if sp.state == StateFrom {
			sp.whereClause = &statement.WhereClause{IsSet: true}
			sp.stmt.Where = sp.whereClause
			sp.nodeStack = []node.ASTNode{}
			sp.opStack = []string{}
			sp.state = StateWhere
			return
		}
		sp.setError(errors.New("[parse error] WHERE clause is in invalid position"))
		return

	case "AND", "OR":
		if sp.state == StateWhere {
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
	case StateSelectColumns:
		sp.setError(errors.New("[parse error] currently only SELECT * is supported"))
		return

	case StateFrom:
		if sp.stmt == nil {
			sp.setError(ErrSelectStmtIsNil)
			return
		}
		sp.stmt.From = *identifier.NewTableId(ident)

	case StateWhere:
		colId := *identifier.NewColumnId(ident)
		sp.nodeStack = append(sp.nodeStack, colId)
	}
}

func (sp *SelectParser) OnSymbol(symbol string) {
	if sp.err != nil {
		return
	}

	switch sp.state {
	case StateSelectColumns:
		if symbol != "*" {
			sp.setError(errors.New("[parse error] currently only SELECT * is supported"))
			return
		}
	case StateWhere:
		sp.handleOperator(symbol)
	default:
		sp.setError(errors.New("[parse error] unexpected symbol: " + symbol))
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

func (sp *SelectParser) handleLiteral(lit literal.Literal) {
	if sp.err != nil {
		return
	}
	if sp.state == StateWhere {
		sp.nodeStack = append(sp.nodeStack, lit)
	}
}

// 演算子を処理する
func (sp *SelectParser) handleOperator(op string) {
	// 新しい演算子を積む前に、スタックにある「優先順位が高い or 同じ」演算子を処理する
	// e.g. スタックに "=" があって、今 "AND" が来た場合 -> 先に "=" を reduce する
	for len(sp.opStack) > 0 {
		topOp := sp.opStack[len(sp.opStack)-1]
		if sp.precedence(topOp) >= sp.precedence(op) {
			if err := sp.reduce(); err != nil {
				sp.setError(err)
				return
			}
		} else {
			break
		}
	}
	sp.opStack = append(sp.opStack, op)
}

// スタックから要素を取り出し、1つの BinaryExpr を作って nodeStack に戻す
// e.g.
// - nodeStack: [name, "john"], opStack: ["="] -> nodeStack: [BinaryExpr(name = "john")]
// - nodeStack: [age, 30, BinaryExpr(name = "john")], opStack: [">", "AND"] -> nodeStack: [BinaryExpr(age > 30 AND name = "john")]
func (sp *SelectParser) reduce() error {
	// 必要な要素が足りているかチェック
	if len(sp.nodeStack) < 2 || len(sp.opStack) < 1 {
		return errors.New("[parse error] invalid expression syntax")
	}

	// 右辺を Pop (スタックはLIFOなので先に右辺が出てくる)
	rightRaw := sp.nodeStack[len(sp.nodeStack)-1]
	sp.nodeStack = sp.nodeStack[:len(sp.nodeStack)-1]

	// 演算子を Pop
	op := sp.opStack[len(sp.opStack)-1]
	sp.opStack = sp.opStack[:len(sp.opStack)-1]

	// 左辺を Pop
	leftRaw := sp.nodeStack[len(sp.nodeStack)-1]
	sp.nodeStack = sp.nodeStack[:len(sp.nodeStack)-1]

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
	sp.nodeStack = append(sp.nodeStack, expr)

	return nil
}

// 演算子の優先順位を定義 (数値が高いほど優先順位が高い)
func (sp *SelectParser) precedence(op string) int {
	switch strings.ToUpper(op) {
	case "=", "<", ">", "<=", ">=", "!=":
		return 2 // 比較演算子
	case "AND":
		return 1 // 論理演算子
	case "OR":
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
