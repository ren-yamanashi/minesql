package parser

import (
	"errors"
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/literal"
	"minesql/internal/planner/ast/node"
	"minesql/internal/planner/ast/statement"
	"strings"
)

// state
const (
	InsertStateStart           ParserState = 500 + iota // INSERT
	InsertStateInto                                     // INTO
	InsertStateTableName                                // table_name
	InsertStateColumnsOrValues                          // ( or VALUES
	InsertStateColumns                                  // col1, col2, ...
	InsertStateValues                                   // VALUES
	InsertStateValueListStart                           // (
	InsertStateValueList                                // val1, val2, ...
)

type InsertParser struct {
	// 現在のステート
	state ParserState
	// 現在構築中の INSERT 文
	stmt *statement.InsertStmt
	// カラムリスト
	cols []identifier.ColumnId
	// 値リスト (現在構築中の行)
	currentRow []literal.Literal
	// 全ての値リスト
	allRows [][]literal.Literal
	// エラー情報
	err error
}

func NewInsertParser() *InsertParser {
	return &InsertParser{
		state: InsertStateStart,
		stmt:  &statement.InsertStmt{},
	}
}

func (ip *InsertParser) getResult() node.ASTNode {
	return ip.stmt
}

func (ip *InsertParser) getError() error {
	return ip.err
}

func (ip *InsertParser) finalize() {
	if ip.err != nil {
		return
	}

	// テーブル名が未設定の場合はエラー
	if ip.stmt.Table.TableName == "" {
		ip.setError(errors.New("[parse error] table name is required"))
		return
	}

	// 現在の行が残っている場合はフラッシュ
	ip.flushCurrentRow()

	// 値が未設定の場合はエラー
	if len(ip.allRows) == 0 {
		ip.setError(errors.New("[parse error] at least one row of values is required"))
		return
	}

	// カラム数と値数の整合性チェック
	if len(ip.cols) > 0 {
		expectedColCount := len(ip.cols)
		for i, row := range ip.allRows {
			if len(row) != expectedColCount {
				ip.setError(errors.New("[parse error] column count mismatch in row " + string(rune(i))))
				return
			}
		}
	}

	// stmt に設定
	ip.stmt.Cols = ip.cols
	ip.stmt.Values = ip.allRows
}

func (ip *InsertParser) OnKeyword(word string) {
	if ip.err != nil {
		return
	}

	upper := strings.ToUpper(word)
	switch ip.state {
	case InsertStateStart:
		if upper == "INSERT" {
			ip.stmt.StmtType = statement.StmtTypeInsert
			ip.state = InsertStateInto
			return
		}
		ip.setError(errors.New("[parse error] expected INSERT, got: " + word))
		return

	case InsertStateInto:
		if upper == "INTO" {
			ip.state = InsertStateTableName
			return
		}
		ip.setError(errors.New("[parse error] expected INTO, got: " + word))
		return

	case InsertStateColumnsOrValues, InsertStateColumns, InsertStateValues:
		if upper == "VALUES" {
			ip.state = InsertStateValueListStart
			return
		}
		ip.setError(errors.New("[parse error] unexpected keyword: " + word))
		return

	default:
		ip.setError(errors.New("[parse error] unexpected keyword: " + word))
		return
	}
}

func (ip *InsertParser) OnIdentifier(ident string) {
	if ip.err != nil {
		return
	}

	switch ip.state {
	case InsertStateTableName:
		ip.stmt.Table = *identifier.NewTableId(ident)
		ip.state = InsertStateColumnsOrValues
		return

	case InsertStateColumns:
		// カラム名を追加
		ip.cols = append(ip.cols, *identifier.NewColumnId(ident))
		return

	default:
		ip.setError(errors.New("[parse error] unexpected identifier: " + ident))
		return
	}
}

func (ip *InsertParser) OnSymbol(symbol string) {
	if ip.err != nil {
		return
	}

	switch ip.state {
	case InsertStateColumnsOrValues:
		if symbol == "(" {
			ip.state = InsertStateColumns
			return
		}
		ip.setError(errors.New("[parse error] expected '(' or VALUES, got: " + symbol))
		return

	case InsertStateColumns:
		if symbol == "," {
			// 次のカラム待ち
			return
		}
		if symbol == ")" {
			ip.state = InsertStateValues
			return
		}
		ip.setError(errors.New("[parse error] unexpected symbol in columns: " + symbol))
		return

	case InsertStateValues:
		if symbol == "(" {
			ip.state = InsertStateValueListStart
			return
		}
		ip.setError(errors.New("[parse error] expected '(', got: " + symbol))
		return

	case InsertStateValueListStart:
		if symbol == "(" {
			// 新しい行の開始
			ip.currentRow = []literal.Literal{}
			ip.state = InsertStateValueList
			return
		}
		if symbol == "," {
			// 次の行の開始待ち (e.g. VALUES (...), (...))
			return
		}
		ip.setError(errors.New("[parse error] expected '(' or ',', got: " + symbol))
		return

	case InsertStateValueList:
		if symbol == "," {
			// 次の値待ち (同じ行内)
			return
		}
		if symbol == ")" {
			// 現在の行を確定
			ip.flushCurrentRow()
			ip.state = InsertStateValueListStart
			return
		}
		ip.setError(errors.New("[parse error] unexpected symbol in values: " + symbol))
		return

	default:
		ip.setError(errors.New("[parse error] unexpected symbol: " + symbol))
		return
	}
}

func (ip *InsertParser) OnString(value string) {
	if ip.err != nil {
		return
	}

	if ip.state == InsertStateValueList {
		ip.currentRow = append(ip.currentRow, literal.NewStringLiteral(value, value))
		return
	}

	ip.setError(errors.New("[parse error] unexpected string: " + value))
}

func (ip *InsertParser) OnNumber(num string) {
	if ip.err != nil {
		return
	}

	if ip.state == InsertStateValueList {
		ip.currentRow = append(ip.currentRow, literal.NewStringLiteral(num, num))
		return
	}

	ip.setError(errors.New("[parse error] unexpected number: " + num))
}

func (ip *InsertParser) OnComment(text string) {
	// 何もしない
}

func (ip *InsertParser) OnError(err error) {
	ip.setError(err)
}

// エラーを設定する (既にエラーが設定されている場合は無視する)
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
