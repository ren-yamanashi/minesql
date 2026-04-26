package parser

import (
	"errors"
	"strings"

	"github.com/ren-yamanashi/minesql/internal/ast"
)

var (
	ErrDeleteStmtIsNil error = errors.New("[internal error] DeleteStmt is nil")
)

type DeleteParser struct {
	state parserState     // 現在のステート
	stmt  *ast.DeleteStmt // 現在構築中の DELETE 文
	where WhereParser     // WHERE 句パーサー
	err   error           // エラー情報
}

func NewDeleteParser() *DeleteParser {
	return &DeleteParser{
		state: DeleteStateDelete,
	}
}

func (dp *DeleteParser) getResult() ast.Statement { return dp.stmt }
func (dp *DeleteParser) getError() error          { return dp.err }
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

func (dp *DeleteParser) onKeyword(word string) {
	if dp.err != nil {
		return
	}
	upperWord := strings.ToUpper(word)

	switch upperWord {
	case KDelete:
		dp.stmt = &ast.DeleteStmt{}
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
		dp.setError(errors.New("[parse error] " + upperWord + " operator is in invalid position"))
		return

	default:
		dp.setError(errors.New("[parse error] unsupported keyword: " + word))
		return
	}
}

func (dp *DeleteParser) onIdentifier(ident string) {
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

func (dp *DeleteParser) onSymbol(symbol string) {
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

func (dp *DeleteParser) onString(value string) {
	if dp.err != nil {
		return
	}
	if dp.state == DeleteStateWhere {
		dp.where.pushLiteral(ast.NewStringLiteral(value))
	}
}

func (dp *DeleteParser) onNumber(num string) {
	if dp.err != nil {
		return
	}
	if dp.state == DeleteStateWhere {
		dp.where.pushLiteral(ast.NewStringLiteral(num))
	}
}

func (dp *DeleteParser) onComment(text string) {}
func (dp *DeleteParser) onError(err error)     { dp.setError(err) }

// setError はエラーを設定する (既にエラーが設定されている場合は無視する)
func (dp *DeleteParser) setError(err error) {
	if dp.err == nil {
		dp.err = err
	}
}
