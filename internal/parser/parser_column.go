package parser

import (
	"errors"
	"minesql/internal/planner/ast/definition"
	"strings"
)

type ColumnParser struct {
	// 現在のステート
	state ParserState
	// 構築中のカラム定義
	colDef *definition.ColumnDef
	// エラー情報
	err error
}

func NewColumnParser(colName string) *ColumnParser {
	return &ColumnParser{
		state:  CreateStateColDataType,
		colDef: &definition.ColumnDef{ColName: colName},
	}
}

func (cp *ColumnParser) finalize() error {
	if cp.err != nil {
		return cp.err
	}
	if cp.colDef.DataType == "" {
		return errors.New("[parse error] data type is required for column: " + cp.colDef.ColName)
	}
	return nil
}

func (cp *ColumnParser) getDef() definition.Definition {
	return cp.colDef
}

func (cp *ColumnParser) OnKeyword(word string) {
	if cp.err != nil {
		return
	}

	upper := strings.ToUpper(word)
	switch cp.state {
	case CreateStateColDataType:
		// 現状、カラムのデータ型は VARCHAR のみ対応
		if upper == KVarchar {
			cp.colDef.DataType = definition.DataTypeVarchar
			cp.state = CreateStateColDefEnd
			return
		}
		cp.setError(errors.New("[parse error] only VARCHAR is supported, got: " + word))
		return

	case CreateStateColDefEnd:
		// 型が決まった後にさらにキーワードが来た場合 (e.g. "col1 VARCHAR INT")
		cp.setError(errors.New("[parse error] unexpected keyword after data type"))
		return
	}
}

func (cp *ColumnParser) OnIdentifier(ident string) { cp.setError(errors.New("unexpected identifier")) }
func (cp *ColumnParser) OnSymbol(sym string)       { cp.setError(errors.New("unexpected symbol")) }
func (cp *ColumnParser) OnString(value string)     { cp.setError(errors.New("unexpected string")) }
func (cp *ColumnParser) OnNumber(num string)       { cp.setError(errors.New("unexpected number")) }
func (cp *ColumnParser) OnComment(text string)     {}
func (cp *ColumnParser) OnError(err error)         { cp.setError(err) }

func (cp *ColumnParser) setError(err error) {
	if cp.err == nil {
		cp.err = err
	}
}
