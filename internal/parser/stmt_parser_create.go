package parser

import (
	"errors"
	"strings"

	"github.com/ren-yamanashi/minesql/internal/ast"
)

type CreateParser struct {
	state     parserState          // 現在のステート
	stmt      *ast.CreateTableStmt // 現在構築中の CREATE TABLE 文
	err       error                // エラー情報
	colParser *ColumnDefParser     // カラム定義のサブパーサー
	conParser *ConstraintDefParser // 制約定義のサブパーサー
}

func NewCreateParser() *CreateParser {
	return &CreateParser{
		state: CreateStateStart,
		stmt:  &ast.CreateTableStmt{},
	}
}

func (cp *CreateParser) getResult() ast.Statement { return cp.stmt }
func (cp *CreateParser) getError() error          { return cp.err }
func (cp *CreateParser) finalize() {
	if cp.err != nil {
		return
	}

	// テーブル名が空の場合はエラー
	if cp.stmt.TableName == "" {
		cp.setError(errors.New("[parse error] table name is required"))
		return
	}
	cp.flushActiveParser()

	// カラム定義が空の場合はエラー
	if len(cp.stmt.CreateDefinitions) == 0 {
		cp.setError(errors.New("[parse error] at least one column definition is required"))
		return
	}

	// ステートが End でない場合はエラー
	if cp.state != CreateStateEnd {
		cp.setError(errors.New("[parse error] incomplete CREATE TABLE statement"))
		return
	}
}

func (cp *CreateParser) onKeyword(word string) {
	if cp.err != nil {
		return
	}
	if cp.colParser != nil {
		cp.colParser.onKeyword(word)
		return
	}
	if cp.conParser != nil {
		cp.conParser.onKeyword(word)
		return
	}

	upper := strings.ToUpper(word)
	switch cp.state {
	case CreateStateStart:
		if upper == KCreate {
			cp.state = CreateStateCreate
			return
		}
	case CreateStateCreate:
		if upper == KTable {
			cp.state = CreateStateTable
			return
		}
	case CreateStateBody:
		if upper == KPrimary || upper == KUnique || upper == KKey || upper == KForeign {
			cp.conParser = NewConstraintDefParser()
			cp.conParser.onKeyword(word)
			return
		}
		cp.setError(errors.New("[parse error] unexpected keyword: " + word))
		return
	default:
		cp.setError(errors.New("[parse error] unexpected keyword: " + word))
		return
	}
}

func (cp *CreateParser) onIdentifier(ident string) {
	if cp.err != nil {
		return
	}
	if cp.conParser != nil {
		cp.conParser.onIdentifier(ident)
		return
	}
	if cp.colParser != nil {
		cp.setError(errors.New("[parse error] unexpected identifier: " + ident))
		return
	}

	switch cp.state {
	case CreateStateTable:
		cp.stmt.TableName = ident
		cp.state = CreateStateBodyStart
		return
	case CreateStateBody:
		cp.colParser = NewColumnDefParser(ident)
		return
	default:
		cp.setError(errors.New("[parse error] unexpected identifier: " + ident))
		return
	}
}

func (cp *CreateParser) onSymbol(symbol string) {
	if cp.err != nil {
		return
	}

	// ";" が来たら state を End にする
	if symbol == string(SSemicolon) {
		cp.flushActiveParser()
		cp.state = CreateStateEnd
		return
	}

	// ConstraintDefParser がアクティブならシンボルを委譲
	if cp.conParser != nil {
		if symbol == string(SComma) || symbol == string(SLeftParen) {
			cp.conParser.onSymbol(symbol)
			return
		}
		if symbol == string(SRightParen) {
			cp.conParser.onSymbol(symbol)
			// FK では FOREIGN KEY fk (col) の ")" の後に REFERENCES ... が続くため、
			// isDone() が true の時だけ flush する
			if cp.conParser.done {
				cp.flushActiveParser()
			}
			return
		}
	}

	// "," と ")" は区切りなので、親が SubParser を終了させる
	if symbol == string(SComma) || symbol == string(SRightParen) {
		cp.flushActiveParser()
		return
	}

	// Body 開始の "(" を処理
	if cp.state == CreateStateBodyStart && symbol == string(SLeftParen) {
		cp.state = CreateStateBody
		return
	}

	cp.setError(errors.New("[parse error] unexpected symbol: " + symbol))
}

func (cp *CreateParser) onString(value string) {
	if cp.err != nil {
		return
	}
	cp.setError(errors.New("[parse error] unexpected string: " + value))
}

func (cp *CreateParser) onNumber(num string) {
	if cp.err != nil {
		return
	}
	cp.setError(errors.New("[parse error] unexpected number: " + num))
}

func (cp *CreateParser) onComment(_ string) {}
func (cp *CreateParser) onError(err error)  { cp.setError(err) }

// エラーを設定する (既にエラーが設定されている場合は無視する)
func (cp *CreateParser) setError(err error) {
	if cp.err == nil {
		cp.err = err
	}
}

// flushActiveParser はサブパーサーを正常終了させ、結果を stmt に取り込む
func (cp *CreateParser) flushActiveParser() {
	switch {
	case cp.colParser != nil:
		if err := cp.colParser.finalize(); err != nil {
			cp.setError(err)
		} else {
			cp.stmt.CreateDefinitions = append(cp.stmt.CreateDefinitions, cp.colParser.getDef())
		}
		cp.colParser = nil
	case cp.conParser != nil:
		if err := cp.conParser.finalize(); err != nil {
			cp.setError(err)
		} else {
			cp.stmt.CreateDefinitions = append(cp.stmt.CreateDefinitions, cp.conParser.getDef())
		}
		cp.conParser = nil
	}
}
