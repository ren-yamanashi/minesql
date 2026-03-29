package parser

import "minesql/internal/ast"

// TransactionParser は BEGIN / COMMIT / ROLLBACK をパースする
//
// これらのステートメントはキーワードのみで構成されるため、状態遷移は不要
type TransactionParser struct {
	stmtType ast.StmtType
	err      error
}

func NewTransactionParser(stmtType ast.StmtType) *TransactionParser {
	return &TransactionParser{stmtType: stmtType}
}

func (p *TransactionParser) OnKeyword(_ string)    {}
func (p *TransactionParser) OnIdentifier(_ string) {}
func (p *TransactionParser) OnNumber(_ string)     {}
func (p *TransactionParser) OnString(_ string)     {}
func (p *TransactionParser) OnSymbol(_ string)     {}
func (p *TransactionParser) OnComment(_ string)    {}
func (p *TransactionParser) OnError(err error)     { p.err = err }

func (p *TransactionParser) getResult() ast.Statement {
	switch p.stmtType {
	case ast.StmtTypeBegin:
		return &ast.BeginStmt{StmtType: ast.StmtTypeBegin}
	case ast.StmtTypeCommit:
		return &ast.CommitStmt{StmtType: ast.StmtTypeCommit}
	case ast.StmtTypeRollback:
		return &ast.RollbackStmt{StmtType: ast.StmtTypeRollback}
	default:
		return nil
	}
}

func (p *TransactionParser) getError() error { return p.err }
func (p *TransactionParser) finalize()       {}
