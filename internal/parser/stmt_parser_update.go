package parser

import (
	"errors"
	"strings"

	"github.com/ren-yamanashi/minesql/internal/ast"
)

type UpdateParser struct {
	state         parserState     // 現在のステート
	stmt          *ast.UpdateStmt // 現在構築中の UPDATE 文
	where         WhereParser     // WHERE 句パーサー
	currentSetCol string          // 現在構築中の SetClause のカラム名
	err           error           // エラー情報
}

func NewUpdateParser() *UpdateParser {
	return &UpdateParser{
		state: UpdateStateUpdate,
	}
}

func (up *UpdateParser) getResult() ast.Statement { return up.stmt }
func (up *UpdateParser) getError() error          { return up.err }
func (up *UpdateParser) finalize() {
	if up.err != nil {
		return
	}

	// UPDATE 文が構築されていない場合はエラー
	if up.stmt == nil {
		up.setError(errors.New("[parse error] must have UPDATE statement"))
		return
	}

	// テーブル名が空の場合はエラー
	if up.stmt.Table.TableName == "" {
		up.setError(errors.New("[parse error] missing table name"))
		return
	}

	// SET 句がない場合はエラー
	if len(up.stmt.SetClauses) == 0 {
		up.setError(errors.New("[parse error] missing SET clause"))
		return
	}

	// ステートが End でない場合はエラー
	if up.state != UpdateStateEnd {
		up.setError(errors.New("[parse error] incomplete UPDATE statement"))
		return
	}

	// WHERE 句を確定
	whereClause, err := up.where.finalizeWhere()
	if err != nil {
		up.setError(err)
		return
	}
	up.stmt.Where = whereClause
}

func (up *UpdateParser) onKeyword(word string) {
	if up.err != nil {
		return
	}
	upperWord := strings.ToUpper(word)

	switch upperWord {
	case KUpdate:
		up.stmt = &ast.UpdateStmt{}
		up.state = UpdateStateUpdate
		return

	case KSet:
		if up.state == UpdateStateTable {
			up.state = UpdateStateSet
			return
		}
		// SET 句が不適切な位置にある場合はエラー
		up.setError(errors.New("[parse error] SET clause is in invalid position"))
		return

	case KWhere:
		if up.state == UpdateStateSetVal {
			up.stmt.Where = up.where.initWhere()
			up.state = UpdateStateWhere
			return
		}
		// WHERE 句が不適切な位置にある場合はエラー
		up.setError(errors.New("[parse error] WHERE clause is in invalid position"))
		return

	case KAnd, KOr:
		if up.state == UpdateStateWhere {
			if err := up.where.handleOperator(upperWord); err != nil {
				up.setError(err)
			}
			return
		}
		// AND/OR が不適切な位置にある場合はエラー
		up.setError(errors.New("[parse error] " + upperWord + " operator is in invalid position"))
		return

	default:
		up.setError(errors.New("[parse error] unsupported keyword: " + word))
		return
	}
}

func (up *UpdateParser) onIdentifier(ident string) {
	if up.err != nil {
		return
	}

	switch up.state {
	case UpdateStateUpdate:
		// テーブル名
		up.stmt.Table = *ast.NewTableId(ident)
		up.state = UpdateStateTable
	case UpdateStateSet:
		// SET 句のカラム名
		up.currentSetCol = ident
		up.state = UpdateStateSetCol
	case UpdateStateWhere:
		up.where.pushColumn(ident)
	default:
		up.setError(errors.New("[parse error] unexpected identifier: " + ident))
	}
}

func (up *UpdateParser) onSymbol(symbol string) {
	if up.err != nil {
		return
	}

	// ";" が来たら state を End にする
	if symbol == string(SSemicolon) {
		up.state = UpdateStateEnd
		return
	}

	switch up.state {
	case UpdateStateSetCol:
		// "=" のみ受け付ける
		if symbol == string(SEqual) {
			up.state = UpdateStateSetEq
			return
		}
		up.setError(errors.New("[parse error] expected '=' after column name in SET clause"))

	case UpdateStateSetVal:
		// "," が来たら次の SetClause のカラム名待ち
		if symbol == string(SComma) {
			up.state = UpdateStateSet
			return
		}
		up.setError(errors.New("[parse error] unexpected symbol in SET clause: " + symbol))

	case UpdateStateWhere:
		if err := up.where.handleOperator(symbol); err != nil {
			up.setError(err)
		}

	default:
		up.setError(errors.New("[parse error] unexpected symbol: " + symbol))
	}
}

func (up *UpdateParser) onString(value string) {
	if up.err != nil {
		return
	}

	switch up.state {
	case UpdateStateSetEq:
		// SET 句の値
		up.stmt.SetClauses = append(up.stmt.SetClauses, &ast.SetClause{
			Column: *ast.NewColumnId(up.currentSetCol),
			Value:  ast.NewStringLiteral(value),
		})
		up.currentSetCol = ""
		up.state = UpdateStateSetVal
	case UpdateStateWhere:
		up.where.pushLiteral(ast.NewStringLiteral(value))
	default:
		up.setError(errors.New("[parse error] unexpected string: " + value))
	}
}

func (up *UpdateParser) onNumber(num string) {
	if up.err != nil {
		return
	}

	switch up.state {
	case UpdateStateSetEq:
		// SET 句の値 (数値)
		up.stmt.SetClauses = append(up.stmt.SetClauses, &ast.SetClause{
			Column: *ast.NewColumnId(up.currentSetCol),
			Value:  ast.NewStringLiteral(num),
		})
		up.currentSetCol = ""
		up.state = UpdateStateSetVal
	case UpdateStateWhere:
		up.where.pushLiteral(ast.NewStringLiteral(num))
	default:
		up.setError(errors.New("[parse error] unexpected number: " + num))
	}
}

func (up *UpdateParser) onComment(text string) {}
func (up *UpdateParser) onError(err error)     { up.setError(err) }

// setError はエラーを設定する (既にエラーが設定されている場合は無視する)
func (up *UpdateParser) setError(err error) {
	if up.err == nil {
		up.err = err
	}
}
