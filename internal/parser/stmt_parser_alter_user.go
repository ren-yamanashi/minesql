package parser

import (
	"fmt"
	"strings"

	"github.com/ren-yamanashi/minesql/internal/ast"
)

// AlterUserParser は ALTER USER 文をパースする
//
// 構文: ALTER USER 'username'@'host' IDENTIFIED BY 'password';
type AlterUserParser struct {
	state    parserState
	username string
	host     string
	password string
	err      error
}

func NewAlterUserParser() *AlterUserParser {
	return &AlterUserParser{}
}

func (p *AlterUserParser) getResult() ast.Statement {
	if p.err != nil {
		return nil
	}
	return &ast.AlterUserStmt{
		Username: p.username,
		Host:     p.host,
		Password: p.password,
	}
}

func (p *AlterUserParser) getError() error { return p.err }

func (p *AlterUserParser) finalize() {
	if p.err != nil {
		return
	}
	if p.state != AlterUserStateEnd {
		p.err = fmt.Errorf("[parse error] incomplete ALTER USER statement")
	}
}

func (p *AlterUserParser) onKeyword(word string) {
	if p.err != nil {
		return
	}

	upper := strings.ToUpper(word)

	switch p.state {
	case AlterUserStateAlter:
		// 初期状態: ALTER の次は USER のみ
		if upper != KUser {
			p.err = fmt.Errorf("[parse error] expected USER after ALTER, got %q", word)
		}
		p.state = AlterUserStateUser

	case AlterUserStateHost:
		// ホスト名の次は IDENTIFIED のみ
		if upper != KIdentified {
			p.err = fmt.Errorf("[parse error] expected IDENTIFIED after host, got %q", word)
		}
		p.state = AlterUserStateIdentified

	case AlterUserStateIdentified:
		// IDENTIFIED の次は BY のみ
		if upper != KBy {
			p.err = fmt.Errorf("[parse error] expected BY after IDENTIFIED, got %q", word)
		}
		p.state = AlterUserStateBy

	default:
		if upper == KAlter {
			p.state = AlterUserStateAlter
			return
		}
		p.err = fmt.Errorf("[parse error] unexpected keyword %q in ALTER USER statement", word)
	}
}

func (p *AlterUserParser) onIdentifier(ident string) {
	if p.err != nil {
		return
	}

	switch p.state {
	case AlterUserStateUsername:
		// @ 記号 (トークナイザは @ を識別子として扱う)
		if ident == "@" {
			p.state = AlterUserStateAt
			return
		}
		p.err = fmt.Errorf("[parse error] expected '@' after username, got %q", ident)

	default:
		p.err = fmt.Errorf("[parse error] unexpected identifier %q in ALTER USER statement", ident)
	}
}

func (p *AlterUserParser) onString(value string) {
	if p.err != nil {
		return
	}

	switch p.state {
	case AlterUserStateUser:
		// USER の次はユーザー名
		p.username = value
		p.state = AlterUserStateUsername

	case AlterUserStateAt:
		// @ の次はホスト名
		p.host = value
		p.state = AlterUserStateHost

	case AlterUserStateBy:
		// BY の次はパスワード
		p.password = value
		p.state = AlterUserStateEnd

	default:
		p.err = fmt.Errorf("[parse error] unexpected string %q in ALTER USER statement", value)
	}
}

func (p *AlterUserParser) onSymbol(symbol string) {
	if p.err != nil {
		return
	}

	switch p.state {
	case AlterUserStateEnd:
		if symbol == ";" {
			return
		}
		p.err = fmt.Errorf("[parse error] expected ';' at end of ALTER USER statement, got %q", symbol)

	default:
		p.err = fmt.Errorf("[parse error] unexpected symbol %q in ALTER USER statement", symbol)
	}
}

func (p *AlterUserParser) onNumber(_ string) {
	if p.err != nil {
		return
	}
	p.err = fmt.Errorf("[parse error] unexpected number in ALTER USER statement")
}

func (p *AlterUserParser) onComment(_ string) {}

func (p *AlterUserParser) onError(err error) {
	p.err = err
}
