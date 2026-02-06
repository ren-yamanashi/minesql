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

// state
type ParserState int

const (
	StateInitial       ParserState = iota
	StateSelectColumns             // SELECT [this] FROM
	StateFrom                      // FROM [this]
	StateWhere                     // WHERE [this]
)

// parser (implements TokenHandler)

type Parser struct {
	// 現在のステート
	state ParserState
	// 現在構築中の AST ノード
	result node.ASTNode
	// 現在構築中の SELECT 文
	currentSelectStmt *statement.SelectStmt
	// 現在構築中の WHERE 句
	currentWhereClause *statement.WhereClause
	// AST ノードが格納されるスタック (Where 句の式構築用)
	nodeStack []node.ASTNode
	// 演算子が格納されるスタック (Where 句の式構築用)
	opStack []string
	// エラー情報
	err error
}

func NewParser(tokenizer *Tokenizer) *Parser {
	return &Parser{
		state: StateInitial,
	}
}

func (p *Parser) Parse(sql string) (node.ASTNode, error) {
	tokenizer := NewTokenizer(sql, p)
	tokenizer.Tokenize()

	if p.err != nil {
		return nil, p.err
	}

	// SQL の解析後、WHERE 句の構築を完了する
	if p.state == StateWhere {
		p.finalizeWhereClause()
	}

	if p.err != nil {
		return nil, p.err
	}

	return p.result, nil
}

func (p *Parser) OnKeyword(word string) {
	if p.err != nil {
		return
	}
	upperWord := strings.ToUpper(word)

	switch upperWord {
	case "SELECT":
		p.currentSelectStmt = &statement.SelectStmt{StmtType: statement.StmtTypeSelect}
		p.result = p.currentSelectStmt
		p.state = StateSelectColumns
		return

	case "FROM":
		if p.state == StateSelectColumns {
			p.state = StateFrom
			return
		}
		p.setError(errors.New("[parse error] FROM clause is in invalid position"))
		return

	case "WHERE":
		if p.state == StateFrom {
			p.currentWhereClause = &statement.WhereClause{IsSet: true}
			p.currentSelectStmt.Where = p.currentWhereClause
			p.nodeStack = []node.ASTNode{}
			p.opStack = []string{}
			p.state = StateWhere
			return
		}
		p.setError(errors.New("[parse error] WHERE clause is in invalid position"))
		return

	case "AND", "OR":
		if p.state == StateWhere {
			p.handleOperator(upperWord)
			return
		}
		p.setError(errors.New("[parse error] AND operator is in invalid position"))
		return

	default:
		p.setError(errors.New("[parse error] unsupported keyword: " + word))
		return
	}
}

func (p *Parser) OnIdentifier(ident string) {
	if p.err != nil {
		return
	}

	switch p.state {
	case StateSelectColumns:
		if p.currentSelectStmt == nil {
			p.setError(ErrSelectStmtIsNil)
			return
		}
		if ident != "*" {
			p.setError(errors.New("[parse error] currently only SELECT * is supported"))
			return
		}

	case StateFrom:
		if p.currentSelectStmt == nil {
			p.setError(ErrSelectStmtIsNil)
			return
		}
		p.currentSelectStmt.From = *identifier.NewTableId(ident)

	case StateWhere:
		colId := *identifier.NewColumnId(ident)
		p.nodeStack = append(p.nodeStack, colId)
	}
}

func (p *Parser) OnSymbol(symbol string) {
	if p.err != nil {
		return
	}

	switch p.state {
	case StateWhere:
		p.handleOperator(symbol)
	}
}

func (p *Parser) OnString(value string) {
	p.handleLiteral(literal.NewStringLiteral(value, value))
}

func (p *Parser) OnNumber(num string) {
	p.handleLiteral(literal.NewStringLiteral(num, num))
}

func (p *Parser) OnComment(text string) {
	// 何もしない
}

func (p *Parser) handleLiteral(lit literal.Literal) {
	if p.err != nil {
		return
	}
	if p.state == StateWhere {
		p.nodeStack = append(p.nodeStack, lit)
	}
}

// 演算子を処理する
func (p *Parser) handleOperator(op string) {
	// 新しい演算子を積む前に、スタックにある「優先順位が高い or 同じ」演算子を処理する
	// e.g. スタックに "=" があって、今 "AND" が来た場合 -> 先に "=" を reduce する
	for len(p.opStack) > 0 {
		topOp := p.opStack[len(p.opStack)-1]
		if p.precedence(topOp) >= p.precedence(op) {
			if err := p.reduce(); err != nil {
				p.setError(err)
				return
			}
		} else {
			break
		}
	}
	p.opStack = append(p.opStack, op)
}

// スタックから要素を取り出し、1つの BinaryExpr を作って nodeStack に戻す
// e.g. 
// - nodeStack: [name, "john"], opStack: ["="] -> nodeStack: [BinaryExpr(name = "john")]
// - nodeStack: [age, 30, BinaryExpr(name = "john")], opStack: [">", "AND"] -> nodeStack: [BinaryExpr(age > 30 AND name = "john")]
func (p *Parser) reduce() error {
	// 必要な要素が足りているかチェック
	if len(p.nodeStack) < 2 || len(p.opStack) < 1 {
		return errors.New("[parse error] invalid expression syntax")
	}

	// 右辺を Pop (スタックはLIFOなので先に右辺が出てくる)
	rightRaw := p.nodeStack[len(p.nodeStack)-1]
	p.nodeStack = p.nodeStack[:len(p.nodeStack)-1]

	// 演算子を Pop
	op := p.opStack[len(p.opStack)-1]
	p.opStack = p.opStack[:len(p.opStack)-1]

	// 左辺を Pop
	leftRaw := p.nodeStack[len(p.nodeStack)-1]
	p.nodeStack = p.nodeStack[:len(p.nodeStack)-1]

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
	p.nodeStack = append(p.nodeStack, expr)

	return nil
}

// WHERE 句の構築を完了する
func (p *Parser) finalizeWhereClause() {
	// 残っている演算子をすべて処理
	for len(p.opStack) > 0 {
		if err := p.reduce(); err != nil {
			p.setError(err)
			return
		}
	}

	// WHERE 句があるのに式が一つもない場合はエラー
	if len(p.nodeStack) == 0 {
		p.setError(errors.New("[parse error] empty expression in WHERE clause"))
		return
	}

	// スタックに複数の要素が残っている場合はエラー
	if len(p.nodeStack) != 1 {
		p.setError(errors.New("[parse error] incomplete expression in WHERE clause"))
		return
	}

	// 最後に残った1つが、完成したルートの Expression
	finalExpr, ok := p.nodeStack[0].(expression.Expression)
	if !ok {
		p.setError(errors.New("[parse error] invalid expression result"))
		return
	}

	p.currentWhereClause.Condition = finalExpr
}

// 演算子の優先順位を定義 (数値が高いほど優先順位が高い)
func (p *Parser) precedence(op string) int {
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
func (p *Parser) setError(err error) {
	if p.err == nil {
		p.err = err
	}
}
