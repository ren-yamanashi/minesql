package parser

import (
	"fmt"
	"strings"

	"github.com/ren-yamanashi/minesql/internal/ast"
)

// TransactionParser は BEGIN / COMMIT / ROLLBACK / START TRANSACTION をパースする
//
// BEGIN, COMMIT, ROLLBACK はキーワードのみで完結する。
// START TRANSACTION は START の後に TRANSACTION キーワードを待つ状態遷移がある。
type TransactionParser struct {
	state parserState
	kind  ast.TransactionKind
	err   error
}

// NewTransactionParser は BEGIN / COMMIT / ROLLBACK 用のパーサーを生成する
//
// これらは単一キーワードで完結するため、完了状態で初期化する
func NewTransactionParser(kind ast.TransactionKind) *TransactionParser {
	return &TransactionParser{kind: kind, state: StartTxStateEnd}
}

// NewStartTransactionParser は START TRANSACTION 用のパーサーを生成する
func NewStartTransactionParser() *TransactionParser {
	return &TransactionParser{kind: ast.TxBegin}
}

func (p *TransactionParser) getResult() ast.Statement {
	if p.err != nil {
		return nil
	}
	return &ast.TransactionStmt{Kind: p.kind}
}

func (p *TransactionParser) getError() error { return p.err }

func (p *TransactionParser) finalize() {
	if p.err != nil {
		return
	}
	if p.state != StartTxStateEnd {
		p.err = fmt.Errorf("[parse error] incomplete START TRANSACTION statement")
	}
}

func (p *TransactionParser) onKeyword(word string) {
	if p.err != nil || p.state == StartTxStateEnd {
		return
	}

	upper := strings.ToUpper(word)

	switch p.state {
	case StartTxStateStart:
		if upper != KTransaction {
			p.err = fmt.Errorf("[parse error] expected TRANSACTION after START, got %q", word)
			return
		}
		p.state = StartTxStateEnd

	default:
		if upper == KStart {
			p.state = StartTxStateStart
			return
		}
		p.err = fmt.Errorf("[parse error] unexpected keyword %q in START TRANSACTION statement", word)
	}
}

func (p *TransactionParser) onIdentifier(ident string) {
	if p.err != nil || p.state == StartTxStateEnd {
		return
	}
	p.err = fmt.Errorf("[parse error] unexpected identifier %q in START TRANSACTION statement", ident)
}

func (p *TransactionParser) onString(value string) {
	if p.err != nil || p.state == StartTxStateEnd {
		return
	}
	p.err = fmt.Errorf("[parse error] unexpected string %q in START TRANSACTION statement", value)
}

func (p *TransactionParser) onSymbol(symbol string) {
	if p.err != nil {
		return
	}
	if p.state == StartTxStateEnd && symbol == ";" {
		return
	}
	p.err = fmt.Errorf("[parse error] unexpected symbol %q in START TRANSACTION statement", symbol)
}

func (p *TransactionParser) onNumber(_ string) {
	if p.err != nil || p.state == StartTxStateEnd {
		return
	}
	p.err = fmt.Errorf("[parse error] unexpected number in START TRANSACTION statement")
}

func (p *TransactionParser) onComment(_ string) {}

func (p *TransactionParser) onError(err error) { p.err = err }
