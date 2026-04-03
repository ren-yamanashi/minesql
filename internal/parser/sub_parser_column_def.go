package parser

import (
	"errors"
	"minesql/internal/ast"
	"strings"
)

type ColumnDefParser struct {
	state  parserState    // 現在のステート
	colDef *ast.ColumnDef // 構築中のカラム定義
	err    error          // エラー情報
}

func NewColumnDefParser(colName string) *ColumnDefParser {
	return &ColumnDefParser{
		state:  CreateStateColDef,
		colDef: &ast.ColumnDef{ColName: colName},
	}
}

func (cp *ColumnDefParser) finalize() error {
	if cp.err != nil {
		return cp.err
	}
	if cp.colDef.DataType == "" {
		return errors.New("[parse error] data type is required for column: " + cp.colDef.ColName)
	}
	return nil
}

func (cp *ColumnDefParser) getDef() ast.Definition {
	return cp.colDef
}

func (cp *ColumnDefParser) onKeyword(word string) {
	if cp.err != nil {
		return
	}

	upper := strings.ToUpper(word)
	switch cp.state {
	case CreateStateColDef:
		// 現状、カラムのデータ型は VARCHAR のみ対応
		if upper == KVarchar {
			cp.colDef.DataType = ast.DataTypeVarchar
			cp.state = CreateStateColWaitDefEnd
			return
		}
		cp.setError(errors.New("[parse error] only VARCHAR is supported, got: " + word))
		return

	case CreateStateColWaitDefEnd:
		// 型が決まった後にさらにキーワードが来た場合 (e.g. "col1 VARCHAR INT")
		cp.setError(errors.New("[parse error] unexpected keyword after data type"))
		return
	}
}

func (cp *ColumnDefParser) setError(err error) {
	if cp.err == nil {
		cp.err = err
	}
}
