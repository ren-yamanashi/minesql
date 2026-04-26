package parser

import (
	"errors"
	"strings"

	"github.com/ren-yamanashi/minesql/internal/ast"
)

var (
	ErrSelectStmtIsNil  error = errors.New("[internal error] SelectStmt is nil")
	ErrWhereClauseIsNil error = errors.New("[internal error] WhereClause is nil")
)

type SelectParser struct {
	state       parserState     // 現在のステート
	stmt        *ast.SelectStmt // 現在構築中の SELECT 文
	where       WhereParser     // WHERE 句パーサー
	on          WhereParser     // ON 句パーサー (JOIN 条件)
	currentJoin *ast.JoinClause // 現在パース中の JOIN 句
	err         error           // エラー情報
}

func NewSelectParser() *SelectParser {
	return &SelectParser{
		state: SelectStateColumns,
	}
}

func (sp *SelectParser) getResult() ast.Statement { return sp.stmt }
func (sp *SelectParser) getError() error          { return sp.err }
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

	// 未確定の JOIN 句があれば確定する
	if err := sp.finalizeCurrentJoin(); err != nil {
		sp.setError(err)
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

func (sp *SelectParser) onKeyword(word string) {
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
		// FROM 句が予期しない位置に来た場合はエラー
		sp.setError(errors.New("[parse error] FROM clause is in invalid position"))
		return

	case KInner:
		// INNER は JOIN の前置キーワード。FROM 後または ON 条件後に来る
		if sp.state == SelectStateFrom || sp.state == SelectStateOn {
			if err := sp.finalizeCurrentJoin(); err != nil {
				sp.setError(err)
				return
			}
			sp.state = SelectStateInner
			return
		}
		sp.setError(errors.New("[parse error] INNER keyword is in invalid position"))
		return

	case KJoin:
		// JOIN は FROM 後、ON 条件後、または INNER 後に来る
		if sp.state == SelectStateInner {
			// INNER JOIN の場合: INNER で既に SelectStateInner に遷移済み
			sp.currentJoin = &ast.JoinClause{}
			sp.state = SelectStateJoin
			return
		}
		if sp.state == SelectStateFrom || sp.state == SelectStateOn {
			sp.beginJoin()
			return
		}
		sp.setError(errors.New("[parse error] JOIN keyword is in invalid position"))
		return

	case KOn:
		if sp.state == SelectStateJoinTable {
			sp.on.initWhere()
			sp.state = SelectStateOn
			return
		}
		sp.setError(errors.New("[parse error] ON keyword is in invalid position"))
		return

	case KWhere:
		if sp.state == SelectStateFrom || sp.state == SelectStateOn {
			if err := sp.finalizeCurrentJoin(); err != nil {
				sp.setError(err)
				return
			}
			sp.stmt.Where = sp.where.initWhere()
			sp.state = SelectStateWhere
			return
		}
		// WHERE 句が予期しない位置に来た場合はエラー
		sp.setError(errors.New("[parse error] WHERE clause is in invalid position"))
		return

	case KAnd, KOr:
		if sp.state == SelectStateWhere {
			if err := sp.where.handleOperator(upperWord); err != nil {
				sp.setError(err)
			}
			return
		}
		if sp.state == SelectStateOn {
			if err := sp.on.handleOperator(upperWord); err != nil {
				sp.setError(err)
			}
			return
		}
		// AND/OR が予期しない位置に来た場合はエラー
		sp.setError(errors.New("[parse error] " + upperWord + " operator is in invalid position"))
		return

	default:
		sp.setError(errors.New("[parse error] unsupported keyword: " + word))
		return
	}
}

func (sp *SelectParser) onIdentifier(ident string) {
	if sp.err != nil {
		return
	}

	switch sp.state {
	case SelectStateColumns:
		// カラム名を追加 (修飾名 "table.column" にも対応)
		colId := parseColumnId(ident)
		sp.stmt.Columns = append(sp.stmt.Columns, colId)
		return
	case SelectStateFrom:
		sp.stmt.From = *ast.NewTableId(ident)
	case SelectStateJoin:
		sp.currentJoin.Table = *ast.NewTableId(ident)
		sp.state = SelectStateJoinTable
	case SelectStateOn:
		sp.on.pushColumn(ident)
	case SelectStateWhere:
		sp.where.pushColumn(ident)
	}
}

func (sp *SelectParser) onSymbol(symbol string) {
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
		if symbol == string(CAsterisk) {
			// SELECT * → Columns は nil のまま (全カラム)
			return
		}
		if symbol == string(SComma) {
			// カラム名の区切り → 次のカラム名を待つ
			return
		}
		sp.setError(errors.New("[parse error] unexpected symbol in SELECT clause: " + symbol))
		return
	case SelectStateFrom:
		// FROM 句ではシンボルは来ないはずなのでエラー
		sp.setError(errors.New("[parse error] unexpected symbol in FROM clause: " + symbol))
		return
	case SelectStateOn:
		if err := sp.on.handleOperator(symbol); err != nil {
			sp.setError(err)
		}
	case SelectStateWhere:
		if err := sp.where.handleOperator(symbol); err != nil {
			sp.setError(err)
		}
	}
}

func (sp *SelectParser) onString(value string) {
	if sp.err != nil {
		return
	}
	switch sp.state {
	case SelectStateOn:
		sp.on.pushLiteral(ast.NewStringLiteral(value))
	case SelectStateWhere:
		sp.where.pushLiteral(ast.NewStringLiteral(value))
	}
}

func (sp *SelectParser) onNumber(num string) {
	if sp.err != nil {
		return
	}
	switch sp.state {
	case SelectStateOn:
		sp.on.pushLiteral(ast.NewStringLiteral(num))
	case SelectStateWhere:
		sp.where.pushLiteral(ast.NewStringLiteral(num))
	}
}

func (sp *SelectParser) onComment(text string) {}
func (sp *SelectParser) onError(err error)     { sp.setError(err) }

// beginJoin は新しい JOIN 句のパースを開始する
//
// 既にパース中の JOIN 句がある場合は先に確定する
func (sp *SelectParser) beginJoin() {
	if err := sp.finalizeCurrentJoin(); err != nil {
		sp.setError(err)
		return
	}
	sp.currentJoin = &ast.JoinClause{}
	sp.state = SelectStateJoin
}

// finalizeCurrentJoin は現在パース中の JOIN 句を確定し、SelectStmt に追加する
func (sp *SelectParser) finalizeCurrentJoin() error {
	if sp.currentJoin == nil {
		return nil
	}

	onClause, err := sp.on.finalizeWhere()
	if err != nil {
		return err
	}
	if onClause == nil {
		return errors.New("[parse error] missing ON clause in JOIN")
	}
	sp.currentJoin.Condition = onClause.Condition
	sp.stmt.Joins = append(sp.stmt.Joins, sp.currentJoin)
	sp.currentJoin = nil
	return nil
}

// setError はエラーを設定する (既にエラーが設定されている場合は無視する)
func (sp *SelectParser) setError(err error) {
	if sp.err == nil {
		sp.err = err
	}
}
