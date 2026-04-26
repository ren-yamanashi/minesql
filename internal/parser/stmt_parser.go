package parser

import (
	"strings"

	"github.com/ren-yamanashi/minesql/internal/ast"
)

type StatementParser interface {
	TokenHandler
	getResult() ast.Statement
	getError() error
	finalize()
}

// parseColumnId は識別子を ColumnId に変換する
//
// "table.column" 形式の修飾名の場合、TableName と ColName に分割する
func parseColumnId(ident string) ast.ColumnId {
	if idx := strings.Index(ident, "."); idx > 0 && idx < len(ident)-1 {
		return ast.ColumnId{TableName: ident[:idx], ColName: ident[idx+1:]}
	}
	return ast.ColumnId{ColName: ident}
}
