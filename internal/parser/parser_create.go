package parser

import (
	"errors"
	"minesql/internal/planner/ast/definition"
	"minesql/internal/planner/ast/node"
	"minesql/internal/planner/ast/statement"
	"strings"
)

const (
	// CREATE 文の開始
	CreateStateStart ParserState = 200 + iota
	// TABLE キーワード
	CreateStateTable
	// CREATE TABLE [this]
	CreateStateName
	// CREATE TABLE 文の Body 部 (カラム定義・制約定義)
	CreateStateBodyStart
	// id INT, ...
	CreateStateBody
)

// -- sub parser --

type CreateSubParser interface {
	TokenHandler
	finalize() error
	GetDef() definition.Definition
}

// -- main parser --

type CreateParser struct {
	// 現在のステート
	state ParserState
	// 現在構築中の CREATE TABLE 文
	stmt *statement.CreateTableStmt
	// エラー情報
	err error
	// サブパーサー
	activeSubParser CreateSubParser
}

func NewCreateParser() *CreateParser {
	return &CreateParser{
		state: CreateStateStart,
		stmt:  &statement.CreateTableStmt{},
	}
}

func (cp *CreateParser) getResult() node.ASTNode {
	return cp.stmt
}

func (cp *CreateParser) getError() error {
	return cp.err
}

func (cp *CreateParser) finalize() {
	if cp.stmt.TableName == "" {
		cp.setError(errors.New("[parse error] table name is required"))
		return
	}
	cp.flushActiveParser()
}

func (cp *CreateParser) OnKeyword(word string) {
	if cp.err != nil {
		return
	}

	if cp.activeSubParser != nil {
		cp.activeSubParser.OnKeyword(word)
		return
	}

	upper := strings.ToUpper(word)
	switch cp.state {
	case CreateStateStart:
		if upper == "CREATE" {
			cp.state = CreateStateTable
			return
		}
	case CreateStateTable:
		if upper == "TABLE" {
			cp.stmt.Keyword = statement.KeywordTable
			cp.state = CreateStateName
			return
		}
	case CreateStateBody:
		if upper == "PRIMARY" || upper == "UNIQUE" {
			cp.activeSubParser = NewConstraintParser()
			cp.activeSubParser.OnKeyword(word)
			return
		}
	default:
		cp.setError(errors.New("[parse error] unexpected keyword: " + word))
		return
	}
}

func (cp *CreateParser) OnIdentifier(ident string) {
	if cp.err != nil {
		return
	}

	if cp.activeSubParser != nil {
		cp.activeSubParser.OnIdentifier(ident)
		return
	}

	switch cp.state {
	case CreateStateName:
		cp.stmt.TableName = ident
		cp.state = CreateStateBodyStart
		return
	case CreateStateBody:
		cp.activeSubParser = NewColumnParser(ident)
		return
	default:
		cp.setError(errors.New("[parse error] unexpected identifier: " + ident))
		return
	}
}

func (cp *CreateParser) OnSymbol(symbol string) {
	if cp.err != nil {
		return
	}
	// 区切り文字以外なら、子パーサーに委譲
	if cp.activeSubParser != nil {
		if _, ok := cp.activeSubParser.(*ConstraintParser); ok {
			// カンマ "," は制約定義内の列挙 (col1, col2) として委譲
			if symbol == "," {
				cp.activeSubParser.OnSymbol(symbol)
				return
			}
			// "(" も委譲
			if symbol == "(" {
				cp.activeSubParser.OnSymbol(symbol)
				return
			}
			// ")" は制約定義の終わりかもしれないので、
			// 一旦委譲してから、親としても flush する
			if symbol == ")" {
				cp.activeSubParser.OnSymbol(symbol)
				cp.flushActiveParser()
				return
			}
		}
	}

	// "," と ")" は区切りなので、親が SubParser を終了させる
	if symbol == "," || symbol == ")" {
		cp.flushActiveParser()
		return
	}

	// Body 開始の "(" を処理
	if cp.state == CreateStateBodyStart && symbol == "(" {
		cp.state = CreateStateBody
		return
	}

	// その他は SubParser に委譲
	if cp.activeSubParser != nil {
		cp.activeSubParser.OnSymbol(symbol)
		return
	}

	cp.setError(errors.New("[parse error] unexpected symbol: " + symbol))
}

func (cp *CreateParser) OnString(value string) {
	if cp.err != nil {
		return
	}

	if cp.activeSubParser != nil {
		cp.activeSubParser.OnString(value)
		return
	}

	// CREATE 文では文字列リテラルは想定していない
	cp.setError(errors.New("[parse error] unexpected string: " + value))
}

func (cp *CreateParser) OnNumber(num string) {
	if cp.err != nil {
		return
	}

	if cp.activeSubParser != nil {
		cp.activeSubParser.OnNumber(num)
		return
	}

	// CREATE 文では数値リテラルは想定していない
	cp.setError(errors.New("[parse error] unexpected number: " + num))
}

func (cp *CreateParser) OnComment(text string) {
	if cp.err != nil {
		return
	}

	if cp.activeSubParser != nil {
		cp.activeSubParser.OnComment(text)
		return
	}

	// 何もしない
}

func (cp *CreateParser) OnError(err error) {
	if cp.err != nil {
		return
	}

	if cp.activeSubParser != nil {
		cp.activeSubParser.OnError(err)
		return
	}

	cp.setError(err)
}

// エラーを設定する (既にエラーが設定されている場合は無視する)
func (cp *CreateParser) setError(err error) {
	if cp.err == nil {
		cp.err = err
	}
}

// サブパーサーを正常終了させ、結果を stmt に取り込む
func (cp *CreateParser) flushActiveParser() {
	if cp.activeSubParser != nil {
		if err := cp.activeSubParser.finalize(); err != nil {
			cp.setError(err)
			return
		}
		cp.stmt.CreateDefinitions = append(cp.stmt.CreateDefinitions, cp.activeSubParser.GetDef())
		cp.activeSubParser = nil
	}
}
