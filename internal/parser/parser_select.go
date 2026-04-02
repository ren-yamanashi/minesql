package parser

import (
	"errors"
	"minesql/internal/ast"
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
	stmt *ast.SelectStmt
	// WHERE 句パーサー
	where WhereParser
	// エラー情報
	err error
}

func NewSelectParser() *SelectParser {
	return &SelectParser{
		state: SelectStateColumns,
	}
}

func (sp *SelectParser) getResult() ast.Statement {
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

	// WHERE 句を確定
	whereClause, err := sp.where.finalizeWhere()
	if err != nil {
		sp.setError(err)
		return
	}
	sp.stmt.Where = whereClause
}

func (sp *SelectParser) OnKeyword(word string) {
	if sp.err != nil {
		return
	}
	upperWord := strings.ToUpper(word)

	switch upperWord {
	case KSelect:
		sp.stmt = &ast.SelectStmt{}
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
			sp.stmt.Where = sp.where.initWhere()
			sp.state = SelectStateWhere
			return
		}
		sp.setError(errors.New("[parse error] WHERE clause is in invalid position"))
		return

	case KAnd, KOr:
		if sp.state == SelectStateWhere {
			if err := sp.where.handleOperator(upperWord); err != nil {
				sp.setError(err)
			}
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
		sp.stmt.From = *ast.NewTableId(ident)
	case SelectStateWhere:
		sp.where.pushColumn(ident)
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
		if err := sp.where.handleOperator(symbol); err != nil {
			sp.setError(err)
		}
	}
}

func (sp *SelectParser) OnString(value string) {
	if sp.err != nil {
		return
	}
	if sp.state == SelectStateWhere {
		sp.where.pushLiteral(ast.NewStringLiteral(value))
	}
}

func (sp *SelectParser) OnNumber(num string) {
	if sp.err != nil {
		return
	}
	if sp.state == SelectStateWhere {
		sp.where.pushLiteral(ast.NewStringLiteral(num))
	}
}

func (sp *SelectParser) OnComment(text string) {
	// 何もしない
}

func (sp *SelectParser) OnError(err error) {
	sp.setError(err)
}

// エラーを設定する (既にエラーが設定されている場合は無視する)
func (sp *SelectParser) setError(err error) {
	if sp.err == nil {
		sp.err = err
	}
}
