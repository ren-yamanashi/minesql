package parser

import (
	"errors"
	"minesql/internal/planner/ast/node"
	"strings"
)

// state
type ParserState int

const (
	// 初期状態
	StateInitial ParserState = iota
	// SELECT 句中
	StateSelectColumns
	// FROM 句中
	StateFrom
	// WHERE 句中
	StateWhere
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

	upper := strings.ToUpper(word)

	switch upper {
	case "SELECT":
		p.currentHandler = NewSelectParser()
		p.currentHandler.OnKeyword(word)
		return

	case "CREATE":
		p.currentHandler = NewCreateParser()
		p.currentHandler.OnKeyword(word)
		return

	case "INSERT":
		p.currentHandler = NewInsertParser()
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
