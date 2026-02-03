package parser

type TokenType string

type Token struct {
	Type    TokenType
	Literal string
}

const (
	EOF          TokenType = "EOF"
	LPAREN       TokenType = "("
	RPAREN       TokenType = ")"
	COMMA        TokenType = ","
	SEMICOLON    TokenType = ";"
	QUOTE        TokenType = "'"
	DOUBLE_QUOTE TokenType = "\""
	ASTERISK     TokenType = "*"
	BLANK        TokenType = " "
	BLANK_LINE   TokenType = "\n"
	BLANK_TAB    TokenType = "\t"

	// CREATE TABLE で使用するトークン
	CREATE  TokenType = "CREATE"
	TABLE   TokenType = "TABLE"
	PRIMARY TokenType = "PRIMARY"
	UNIQUE  TokenType = "UNIQUE"
	KEY     TokenType = "KEY"

	// INSERT で使用するトークン
	INSERT TokenType = "INSERT"
	INTO   TokenType = "INTO"
	VALUES TokenType = "VALUES"

	// SELECT で使用するトークン
	SELECT TokenType = "SELECT"
	FROM   TokenType = "FROM"
	WHERE  TokenType = "WHERE"

	// カラム型
	VARCHAR TokenType = "VARCHAR"

	// 演算子
	EQ    TokenType = "="
	LT    TokenType = "<"
	GT    TokenType = ">"
	LEQ   TokenType = "<="
	GEQ   TokenType = ">="
	NOTEQ TokenType = "!="
	AND   TokenType = "AND"

	// キーワード以外
	IDENT   TokenType = "IDENT"
	STRING  TokenType = "STRING"
	NUMBER  TokenType = "NUMBER"
	ILLEGAL TokenType = "ILLEGAL"
)

func NewToken(tokenType TokenType, ch string) Token {
	return Token{Type: tokenType, Literal: ch}
}
