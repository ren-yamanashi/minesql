package parser

import (
	"strings"
)

type Lexer struct {
	// 解析対象の入力文字列 (SQL 全体)
	input       []rune
	pos         int
	nextReadPos int
	ch          rune
}

func NewLexer(input string) *Lexer {
	l := &Lexer{
		input:       []rune(input),
		pos:         0,
		nextReadPos: 0,
		ch:          0,
	}
	l.readChar()
	return l
}

func (l *Lexer) Start() []Token {
	tokens := []Token{}
	for {
		token := l.nextToken()
		tokens = append(tokens, token)
		if token.Type == EOF {
			break
		}
	}
	return tokens
}

func (l *Lexer) nextToken() Token {
	var tok Token

	l.skipWhitespace()

	switch l.ch {
	case '(':
		tok = NewToken(LPAREN, string(l.ch))
	case ')':
		tok = NewToken(RPAREN, string(l.ch))
	case ',':
		tok = NewToken(COMMA, string(l.ch))
	case ';':
		tok = NewToken(SEMICOLON, string(l.ch))
	case '\'':	
		tok.Literal = l.readQuotedLiteral('\'')
		tok.Type = STRING
	case '"':
		tok.Literal = l.readQuotedLiteral('"')
		tok.Type = STRING
	case '*':
		tok = NewToken(ASTERISK, string(l.ch))
	case '=':
		if l.peekChar() == '<' {
			ch := l.ch
			l.readChar()
			tok = NewToken(LEQ, string(ch)+string(l.ch))
		} else if l.peekChar() == '>' {
			ch := l.ch
			l.readChar()
			tok = NewToken(NOTEQ, string(ch)+string(l.ch))
		}
	case '<':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = NewToken(LEQ, string(ch)+string(l.ch))
		} else {
			tok = NewToken(LT, string(l.ch))
		}
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = NewToken(GEQ, string(ch)+string(l.ch))
		} else {
			tok = NewToken(GT, string(l.ch))
		}
	case '!':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = NewToken(NOTEQ, string(ch)+string(l.ch))
		} else {
			tok = NewToken(ILLEGAL, string(l.ch))
		}
	case 0:
		tok.Literal = ""
		tok.Type = EOF
	default:
		if l.isLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			tok.Type = l.lookupIdent(tok.Literal)
			return tok
		}
		if l.isDigit(l.ch) {
			return l.readNumberOrIdentifier()
		}
		tok = NewToken(ILLEGAL, string(l.ch))
	}

	l.readChar()
	return tok
}

// 次の文字を読み込む
func (l *Lexer) readChar() {
	// 入力の終端に達した場合
	if l.nextReadPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.nextReadPos]
	}
	l.pos = l.nextReadPos
	l.nextReadPos++
}

// 次に読む文字を覗き見する
func (l *Lexer) peekChar() rune {
	if l.nextReadPos >= len(l.input) {
		return 0
	}
	return l.input[l.nextReadPos]
}

// 識別子を読み込む
func (l *Lexer) readIdentifier() string {
	startPos := l.pos
	for l.isLetter(l.ch) || l.isDigit(l.ch) {
		l.readChar()
	}
	return string(l.input[startPos:l.pos])
}

// クォートで囲まれた文字列を読み込む
// quoteCh: '\” or '"'
func (l *Lexer) readQuotedLiteral(quoteCh rune) string {
	// 最初のクォートをスキップ
	l.readChar()
	startPos := l.pos

	for {
		l.readChar()
		if l.ch == quoteCh || l.ch == 0 {
			break
		}
	}

	return string(l.input[startPos:l.pos])
}

// 数字で始まったトークンを読み込む
// 数字だけで構成される場合は INT, 英字が混ざれば IDENT (e.g. "2sample") とする
func (l *Lexer) readNumberOrIdentifier() Token {
	startPos := l.pos
	isAllDigits := true

	for l.isDigit(l.ch) || l.isLetter(l.ch) {
		if l.isLetter(l.ch) {
			isAllDigits = false
		}
		l.readChar()
	}

	literal := string(l.input[startPos:l.pos])

	if isAllDigits {
		return NewToken(NUMBER, literal)
	}
	return NewToken(IDENT, literal)
}

// 空白文字をスキップする
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// 文字が英字 or アンダースコアかを判定する
func (l *Lexer) isLetter(ch rune) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

// 文字が数字かを判定する
func (l *Lexer) isDigit(ch rune) bool {
	return '0' <= ch && ch <= '9'
}

// 識別子の TokenType を判定する
func (l *Lexer) lookupIdent(ident string) TokenType {
	lowerIdent := strings.ToLower(ident)
	switch lowerIdent {
	case "create":
		return CREATE
	case "table":
		return TABLE
	case "primary":
		return PRIMARY
	case "unique":
		return UNIQUE
	case "key":
		return KEY
	case "insert":
		return INSERT
	case "into":
		return INTO
	case "values":
		return VALUES
	case "select":
		return SELECT
	case "from":
		return FROM
	case "where":
		return WHERE
	case "varchar":
		return VARCHAR
	case "and":
		return AND
	default:
		return IDENT
	}
}
