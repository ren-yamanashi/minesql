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

	// -- SELECT Statement --

	// SELECT 中
	StateSelectColumns
	// FROM 中
	StateFrom
	// WHERE 中
	StateWhere

	// -- INSERT Statement --

	// INSERT 中
	InsertStateStart
	// INTO 中
	InsertStateInto
	// INSERT INTO のテーブル名中
	InsertStateTableName
	// INSERT のカラムリスト開始待ち
	// `INSERT INTO <table_name>` の後の "(" を待つ状態
	InsertStateColumnListStart
	// INSERT のカラムリスト中
	InsertStateColumns
	// INSERT の VALUES キーワード待ち
	InsertStateValues
	// INSERT の値リスト開始中
	// `INSERT INTO ... VALUES ( ... )` の "(" 中
	InsertStateValueListStart
	// INSERT の値リスト中
	// `INSERT INTO ... VALUES val1, val2, ...` の各値 (val1, val2, ...) 中
	InsertStateValueList

	// -- CREATE Statement --
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
