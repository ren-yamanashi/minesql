package parser

import "minesql/internal/ast"

// TransactionParser は BEGIN / COMMIT / ROLLBACK をパースする
//
// これらのステートメントはキーワードのみで構成されるため、状態遷移は不要
type TransactionParser struct {
	kind ast.TransactionKind
	err  error
}

func NewTransactionParser(kind ast.TransactionKind) *TransactionParser {
	return &TransactionParser{kind: kind}
}

func (p *TransactionParser) OnKeyword(_ string)    {}
func (p *TransactionParser) OnIdentifier(_ string) {}
func (p *TransactionParser) OnNumber(_ string)     {}
func (p *TransactionParser) OnString(_ string)     {}
func (p *TransactionParser) OnSymbol(_ string)     {}
func (p *TransactionParser) OnComment(_ string)    {}
func (p *TransactionParser) OnError(err error)     { p.err = err }

func (p *TransactionParser) getResult() ast.Statement {
	return &ast.TransactionStmt{Kind: p.kind}
}

func (p *TransactionParser) getError() error { return p.err }
func (p *TransactionParser) finalize()       {}
