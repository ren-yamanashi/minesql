package parser

import (
	"errors"
	"minesql/internal/ast"
	"strings"
)

var (
	ErrDeleteStmtIsNil error = errors.New("[internal error] DeleteStmt is nil")
)

type DeleteParser struct {
	// 現在のステート
	state ParserState
	// 現在構築中の DELETE 文
	stmt *ast.DeleteStmt
	// WHERE 句パーサー
	where WhereParser
	// エラー情報
	err error
}

func NewDeleteParser() *DeleteParser {
	return &DeleteParser{
		state: DeleteStateDelete,
	}
}

func (dp *DeleteParser) getResult() ast.Statement {
	return dp.stmt
}

func (dp *DeleteParser) getError() error {
	return dp.err
}

func (dp *DeleteParser) finalize() {
	if dp.err != nil {
		return
	}

	// DELETE 文がない場合はエラー
	if dp.stmt == nil {
		dp.setError(errors.New("[parse error] must have DELETE statement"))
		return
	}

	// テーブル名が空の場合はエラー (FROM 句がない場合を含む)
	if dp.stmt.From.TableName == "" {
		dp.setError(errors.New("[parse error] missing FROM clause"))
		return
	}

	// ステートが End でない場合はエラー
	if dp.state != DeleteStateEnd {
		dp.setError(errors.New("[parse error] incomplete DELETE statement"))
		return
	}

	// WHERE 句を確定
	whereClause, err := dp.where.finalizeWhere()
	if err != nil {
		dp.setError(err)
		return
	}
	dp.stmt.Where = whereClause
}

func (dp *DeleteParser) OnKeyword(word string) {
	if dp.err != nil {
		return
	}
	upperWord := strings.ToUpper(word)

	switch upperWord {
	case KDelete:
		dp.stmt = &ast.DeleteStmt{StmtType: ast.StmtTypeDelete}
		dp.state = DeleteStateDelete
		return

	case KFrom:
		if dp.state == DeleteStateDelete {
			dp.state = DeleteStateFrom
			return
		}
		dp.setError(errors.New("[parse error] FROM clause is in invalid position"))
		return

	case KWhere:
		if dp.state == DeleteStateFrom {
			dp.stmt.Where = dp.where.initWhere()
			dp.state = DeleteStateWhere
			return
		}
		dp.setError(errors.New("[parse error] WHERE clause is in invalid position"))
		return

	case KAnd, KOr:
		if dp.state == DeleteStateWhere {
			if err := dp.where.handleOperator(upperWord); err != nil {
				dp.setError(err)
			}
			return
		}
		dp.setError(errors.New("[parse error] AND operator is in invalid position"))
		return

	default:
		dp.setError(errors.New("[parse error] unsupported keyword: " + word))
		return
	}
}

func (dp *DeleteParser) OnIdentifier(ident string) {
	if dp.err != nil {
		return
	}

	switch dp.state {
	case DeleteStateFrom:
		dp.stmt.From = *ast.NewTableId(ident)
	case DeleteStateWhere:
		dp.where.pushColumn(ident)
	default:
		dp.setError(errors.New("[parse error] unexpected identifier: " + ident))
	}
}

func (dp *DeleteParser) OnSymbol(symbol string) {
	if dp.err != nil {
		return
	}

	// ";" が来たら state を End にする
	if symbol == string(SSemicolon) {
		dp.state = DeleteStateEnd
		return
	}

	switch dp.state {
	case DeleteStateWhere:
		if err := dp.where.handleOperator(symbol); err != nil {
			dp.setError(err)
		}
	default:
		dp.setError(errors.New("[parse error] unexpected symbol: " + symbol))
	}
}

func (dp *DeleteParser) OnString(value string) {
	if dp.err != nil {
		return
	}
	if dp.state == DeleteStateWhere {
		dp.where.pushLiteral(ast.NewStringLiteral(value, value))
	}
}

func (dp *DeleteParser) OnNumber(num string) {
	if dp.err != nil {
		return
	}
	if dp.state == DeleteStateWhere {
		dp.where.pushLiteral(ast.NewStringLiteral(num, num))
	}
}

func (dp *DeleteParser) OnComment(text string) {
	// 何もしない
}

func (dp *DeleteParser) OnError(err error) {
	dp.setError(err)
}

// エラーを設定する (既にエラーが設定されている場合は無視する)
func (dp *DeleteParser) setError(err error) {
	if dp.err == nil {
		dp.err = err
	}
}
