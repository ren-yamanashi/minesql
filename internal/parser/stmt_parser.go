package parser

import "minesql/internal/ast"

type StatementParser interface {
	TokenHandler
	getResult() ast.Statement
	getError() error
	finalize()
}
