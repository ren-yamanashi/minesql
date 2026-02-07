package parser

import (
	"errors"
	"minesql/internal/planner/ast/node"
	"strings"
)

var (
	ErrSelectStmtIsNil  error = errors.New("[internal error] SelectStmt is nil")
	ErrWhereClauseIsNil error = errors.New("[internal error] WhereClause is nil")
)

// state
type ParserState int

const (
	StateInitial            ParserState = iota
	StateCreateTable                    // CREATE TABLE [this] (...)
	StateCreateTableColumns             // CREATE TABLE ... ( [this]
	StateInsertTable                    // INSERT INTO [this] (...)
	StateInsertValues                   // INSERT INTO ... VALUES [this] (...)
)

// parser (implements TokenHandler)

type StatementParser interface {
	TokenHandler
	getResult() node.ASTNode
	getError() error
	finalize()
}

type Parser struct {
	// 現在のステートに対応するハンドラ
	currentHandler StatementParser
}

func NewParser() *Parser {
	return &Parser{}
}

func (p *Parser) Parse(sql string) (node.ASTNode, error) {
	tokenizer := NewTokenizer(sql, p)
	tokenizer.Tokenize()

	if p.currentHandler == nil {
		return nil, errors.New("[parse error] no statement parsed")
	}

	p.currentHandler.finalize()

	if err := p.currentHandler.getError(); err != nil {
		return nil, err
	}

	return p.currentHandler.getResult(), nil
}

func (p *Parser) OnKeyword(word string) {
	if p.currentHandler != nil {
		p.currentHandler.OnKeyword(word)
		return
	}

	upperWord := strings.ToUpper(word)

	switch upperWord {
	case "SELECT":
		p.currentHandler = NewSelectParser()
		p.currentHandler.OnKeyword(word)
		return
	}
}

func (p *Parser) OnIdentifier(ident string) {
	if p.currentHandler != nil {
		p.currentHandler.OnIdentifier(ident)
		return
	}
}

func (p *Parser) OnSymbol(symbol string) {
	if p.currentHandler != nil {
		p.currentHandler.OnSymbol(symbol)
		return
	}
}

func (p *Parser) OnString(value string) {
	if p.currentHandler != nil {
		p.currentHandler.OnString(value)
		return
	}
}

func (p *Parser) OnNumber(num string) {
	if p.currentHandler != nil {
		p.currentHandler.OnNumber(num)
		return
	}
}

func (p *Parser) OnComment(text string) {
	if p.currentHandler != nil {
		p.currentHandler.OnComment(text)
		return
	}
}

func (p *Parser) OnError(err error) {
	if p.currentHandler != nil {
		p.currentHandler.OnError(err)
		return
	}
}
