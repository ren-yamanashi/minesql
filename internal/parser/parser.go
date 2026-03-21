package parser

import (
	"errors"
	"minesql/internal/ast"
	"strings"
)

// state
type ParserState int

const (
	// 初期状態
	StateInitial ParserState = iota

	//
	// -- SELECT Statement --
	//

	// SELECT 中
	SelectStateColumns
	// FROM 中
	SelectStateFrom
	// WHERE 中
	SelectStateWhere
	// SELECT Statement の終わり
	SelectStateEnd

	//
	// -- INSERT Statement --
	//

	// INSERT Statement の開始状態
	InsertStateStart
	// INSERT 中であり、INTO キーワード待ちの状態
	InsertStateInsert
	// INSERT INTO 中であり、テーブル名待ちの状態
	InsertStateInto
	// INSERT INTO <table_name> 中であり、カラムリスト開始 (`INSERT INTO <table_name>` の後の "(") 待ちの状態
	InsertStateTbName
	// INSERT のカラムリスト中
	InsertStateColumns
	// INSERT のカラムリストが修了し、VALUES キーワード待ちの状態
	InsertStateEndCols
	// INSERT の VALUES の値指定中 (`INSERT INTO ... VALUES ( ... )` の "(" 中) の状態
	InsertStateValues
	// INSERT の値リスト中
	// `INSERT INTO ... VALUES (val1, val2, ...)` の各値 (val1, val2, ...) の指定中の状態
	InsertStateValueList
	// INSERT Statement の終わり
	InsertStateEnd

	//
	// -- CREATE Statement --
	//

	// CREATE Statement の開始状態
	CreateStateStart
	// CREATE 中であり、TABLE キーワード待ちの状態
	CreateStateCreate
	// CREATE TABLE 中であり、テーブル名待ちの状態
	CreateStateTable
	// CREATE TABLE の Body 部開始中 (CREATE TABLE <table_name> (...) の "(" 待ち) の状態
	CreateStateBodyStart
	// CREATE TABLE の Body 部中
	CreateStateBody
	// CREATE TABLE のカラム指定中であり、データ型定義待ちの状態
	CreateStateColDef
	// CREATE TABLE のカラム定義修了待ち
	CreateStateColWaitDefEnd
	// CREATE TABLE の KEY 制約中
	CreateStateConstraint
	// CREATE TABLE の PRIMARY KEY または UNIQUE KEY (現在は KEY キーワードの直後) の状態
	// `UNIQUE KEY index_name` の "index_name" または `PRIMARY KEY (...)` の "(" 待ち
	CreateStateConstraintKey
	// CREATE TABLE の KEY 制約のカラム名を指定中 (または指定待ち) の状態
	// `PRIMARY KEY (col1, col2, ...)` または `UNIQUE KEY index_name (col1, col2, ...)` の "(" の直後か、"," の直後の状態
	CreateStateConstraintCol
	// CREATE TABLE の KEY 制約のカラムリスト区切り文字 ("," または ")") 待ち
	CreateStateConstraintWaitSeparator
	// CREATE Statement の終わり
	CreateStateEnd

	//
	// -- DELETE Statement --
	//

	// DELETE 中であり、FROM キーワード待ちの状態
	DeleteStateDelete
	// DELETE FROM 中であり、テーブル名待ちの状態
	DeleteStateFrom
	// DELETE の WHERE 中
	DeleteStateWhere
	// DELETE Statement の終わり
	DeleteStateEnd

	//
	// -- UPDATE Statement --
	//

	// UPDATE 中であり、テーブル名待ちの状態
	UpdateStateUpdate
	// UPDATE <table_name> 中であり、SET キーワード待ちの状態
	UpdateStateTable
	// SET 句のカラム名待ちの状態
	UpdateStateSet
	// SET 句のカラム名の後、"=" 待ちの状態
	UpdateStateSetCol
	// SET 句の "=" の後、値待ちの状態
	UpdateStateSetEq
	// SET 句の値の後、"," or WHERE or ";" 待ちの状態
	UpdateStateSetVal
	// UPDATE の WHERE 中
	UpdateStateWhere
	// UPDATE Statement の終わり
	UpdateStateEnd
)

// parser (implements TokenHandler)

type StatementParser interface {
	TokenHandler
	getResult() ast.Statement
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

func (p *Parser) Parse(sql string) (ast.Statement, error) {
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
	case KSelect:
		p.currentHandler = NewSelectParser()
		p.currentHandler.OnKeyword(word)
		return

	case KCreate:
		p.currentHandler = NewCreateParser()
		p.currentHandler.OnKeyword(word)
		return

	case KInsert:
		p.currentHandler = NewInsertParser()
		p.currentHandler.OnKeyword(word)
		return

	case KDelete:
		p.currentHandler = NewDeleteParser()
		p.currentHandler.OnKeyword(word)
		return

	case KUpdate:
		p.currentHandler = NewUpdateParser()
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
