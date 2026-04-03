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

func (p *TransactionParser) getResult() ast.Statement { return &ast.TransactionStmt{Kind: p.kind} }
func (p *TransactionParser) getError() error          { return p.err }
func (p *TransactionParser) finalize()                {}

func (p *TransactionParser) onKeyword(_ string)    {}
func (p *TransactionParser) onIdentifier(_ string) {}
func (p *TransactionParser) onNumber(_ string)     {}
func (p *TransactionParser) onString(_ string)     {}
func (p *TransactionParser) onSymbol(_ string)     {}
func (p *TransactionParser) onComment(_ string)    {}
func (p *TransactionParser) onError(err error)     { p.err = err }
