package parser

import (
	"errors"
	"minesql/internal/ast"
	"strings"
)

type InsertParser struct {
	state      parserState     // 現在のステート
	stmt       *ast.InsertStmt // 現在構築中の INSERT 文
	cols       []ast.ColumnId  // カラムリスト
	currentRow []ast.Literal   // 値リスト (現在構築中の行)
	allRows    [][]ast.Literal // 全ての値リスト
	err        error           // エラー情報
}

func NewInsertParser() *InsertParser {
	return &InsertParser{
		state: InsertStateStart,
		stmt:  &ast.InsertStmt{},
	}
}

func (ip *InsertParser) getResult() ast.Statement { return ip.stmt }
func (ip *InsertParser) getError() error          { return ip.err }
func (ip *InsertParser) finalize() {
	if ip.err != nil {
		return
	}

	// テーブル名が未設定の場合はエラー
	if ip.stmt.Table.TableName == "" {
		ip.setError(errors.New("[parse error] table name is required"))
		return
	}

	// カラムリストが空の場合はエラー
	if len(ip.cols) == 0 {
		ip.setError(errors.New("[parse error] column list is required"))
		return
	}

	// 現在の行が残っている場合はフラッシュ
	ip.flushCurrentRow()

	// 値が未設定の場合はエラー
	if len(ip.allRows) == 0 {
		ip.setError(errors.New("[parse error] at least one row of values is required"))
		return
	}

	// ステートが End でない場合はエラー
	if ip.state != InsertStateEnd {
		ip.setError(errors.New("[parse error] incomplete INSERT statement"))
		return
	}

	// stmt に設定
	ip.stmt.Cols = ip.cols
	ip.stmt.Values = ip.allRows
}

func (ip *InsertParser) onKeyword(word string) {
	if ip.err != nil {
		return
	}

	upper := strings.ToUpper(word)
	switch ip.state {
	case InsertStateStart:
		// 最初のキーワードは INSERT である必要がある
		if upper == KInsert {
			ip.state = InsertStateInsert
			return
		}
		// 最初のキーワードが INSERT でない場合はエラー
		ip.setError(errors.New("[parse error] expected INSERT, got: " + word))
		return

	case InsertStateInsert:
		// INSERT の次のキーワードは INTO である必要がある
		if upper == KInto {
			ip.state = InsertStateInto
			return
		}
		// INTO が来ない場合はエラー
		ip.setError(errors.New("[parse error] expected INTO, got: " + word))
		return

	case InsertStateTbName:
		// カラムリスト開始前にキーワードが来た場合はエラー
		ip.setError(errors.New("[parse error] column list is required"))
		return

	case InsertStateColumns, InsertStateEndCols:
		if upper == KValues {
			ip.state = InsertStateValues
			return
		}
		// VALUES が来ない場合はエラー
		ip.setError(errors.New("[parse error] unexpected keyword: " + word))
		return

	default:
		ip.setError(errors.New("[parse error] unexpected keyword: " + word))
		return
	}
}

func (ip *InsertParser) onIdentifier(ident string) {
	if ip.err != nil {
		return
	}

	switch ip.state {
	case InsertStateInto:
		ip.stmt.Table = *ast.NewTableId(ident)
		ip.state = InsertStateTbName
		return
	case InsertStateColumns:
		ip.cols = append(ip.cols, *ast.NewColumnId(ident))
		return
	default:
		ip.setError(errors.New("[parse error] unexpected identifier: " + ident))
		return
	}
}

func (ip *InsertParser) onSymbol(symbol string) {
	if ip.err != nil {
		return
	}

	// ";" が来たら state を End にする
	if symbol == string(SSemicolon) {
		ip.state = InsertStateEnd
		return
	}

	switch ip.state {
	case InsertStateTbName:
		// テーブル名の後は必ずカラムリストの開始 "(" が来る
		if symbol == string(SLeftParen) {
			ip.state = InsertStateColumns
			return
		}
		ip.setError(errors.New("[parse error] expected '(', got: " + symbol))
		return

	case InsertStateColumns:
		if symbol == string(SComma) {
			// 次のカラム待ち
			return
		}
		if symbol == string(SRightParen) {
			// カラムリストを修了して、VALUES キーワードを待つ
			ip.state = InsertStateEndCols
			return
		}
		ip.setError(errors.New("[parse error] unexpected symbol in columns: " + symbol))
		return

	case InsertStateValues:
		if symbol == string(SLeftParen) {
			// 新しい行の開始
			ip.currentRow = []ast.Literal{}
			ip.state = InsertStateValueList
			return
		}
		if symbol == string(SComma) {
			// 次の行の開始待ち (e.g. VALUES (...), (...))
			return
		}
		ip.setError(errors.New("[parse error] expected '(' or ',', got: " + symbol))
		return

	case InsertStateValueList:
		if symbol == string(SComma) {
			// 次の値待ち (同じ行内)
			return
		}
		if symbol == string(SRightParen) {
			// 現在の行を確定
			ip.flushCurrentRow()
			ip.state = InsertStateValues
			return
		}
		ip.setError(errors.New("[parse error] unexpected symbol in values: " + symbol))
		return

	default:
		ip.setError(errors.New("[parse error] unexpected symbol: " + symbol))
		return
	}
}

func (ip *InsertParser) onString(value string) {
	if ip.err != nil {
		return
	}
	if ip.state == InsertStateValueList {
		ip.currentRow = append(ip.currentRow, ast.NewStringLiteral(value))
		return
	}
	ip.setError(errors.New("[parse error] unexpected string: " + value))
}

func (ip *InsertParser) onNumber(num string) {
	if ip.err != nil {
		return
	}
	if ip.state == InsertStateValueList {
		ip.currentRow = append(ip.currentRow, ast.NewStringLiteral(num))
		return
	}
	ip.setError(errors.New("[parse error] unexpected number: " + num))
}

func (ip *InsertParser) onComment(text string) {}
func (ip *InsertParser) onError(err error)     { ip.setError(err) }

// setError はエラーを設定する (既にエラーが設定されている場合は無視する)
func (ip *InsertParser) setError(err error) {
	if ip.err == nil {
		ip.err = err
	}
}

// 現在の行を確定して allRows に追加する
func (ip *InsertParser) flushCurrentRow() {
	if len(ip.currentRow) > 0 {
		ip.allRows = append(ip.allRows, ip.currentRow)
		ip.currentRow = nil
	}
}
