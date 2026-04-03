package parser

import "strings"

type state int

const (
	StateData           state = iota // 通常状態
	StateInSingleQuote               // シングルクォート内 ('...')
	StateInDoubleQuote               // ダブルクォート内 ("...")
	StateInLineComment               // 行コメント内 (-- ...)
	StateInBlockComment              // ブロックコメント内 (/* ... */)
)

// Char Code
const (
	CSingleQuote    rune = '\''
	CDoubleQuote    rune = '"'
	CDash           rune = '-'
	CSlash          rune = '/'
	CAsterisk       rune = '*'
	CSpace          rune = ' '
	CTab            rune = '\t'
	CNewLine        rune = '\n'
	CCarriageReturn rune = '\r'
)

// Symbol
const (
	SLeftParen   rune = '('
	SRightParen  rune = ')'
	SComma       rune = ','
	SSemicolon   rune = ';'
	SEqual       rune = '='
	SLessThan    rune = '<'
	SGreaterThan rune = '>'
	SExclamation rune = '!'
	SAsterisk    rune = '*'
)

// Keyword
const (
	KSelect   = "SELECT"
	KFrom     = "FROM"
	KWhere    = "WHERE"
	KInsert   = "INSERT"
	KInto     = "INTO"
	KValues   = "VALUES"
	KCreate   = "CREATE"
	KTable    = "TABLE"
	KPrimary  = "PRIMARY"
	KUnique   = "UNIQUE"
	KKey      = "KEY"
	KVarchar  = "VARCHAR"
	KDelete   = "DELETE"
	KUpdate   = "UPDATE"
	KSet      = "SET"
	KAnd      = "AND"
	KOr       = "OR"
	KBegin    = "BEGIN"
	KCommit   = "COMMIT"
	KRollback = "ROLLBACK"
)

type TokenHandler interface {
	// SELECT, FROM, INSERT, CREATE などのキーワード
	onKeyword(word string)
	// 識別子 (テーブル名、カラム名など) (e.g. users, id, name)
	onIdentifier(ident string)
	// 文字列リテラル
	onString(value string)
	// 数値リテラル
	onNumber(num string)
	// (, ), =, >= などの記号
	onSymbol(symbol string)
	// 行コメント、ブロックコメント
	onComment(text string)
	// エラー
	onError(err error)
}

type Tokenizer struct {
	callbacks TokenHandler // イベントの通知先
	state     state        // 現在のステート
	input     []rune       // 解析対象の入力文字列
	pos       int          // 現在の読み取り位置
	start     int          // 現在のトークン開始位置
}

func NewTokenizer(input string, callbacks TokenHandler) *Tokenizer {
	return &Tokenizer{
		callbacks: callbacks,
		state:     StateData,
		input:     []rune(input),
		pos:       0,
		start:     0,
	}
}

// Tokenize は input を走査し、現在の状態に対応する関数を呼び出す
func (t *Tokenizer) Tokenize() {
	for t.shouldContinue() {
		switch t.state {
		case StateData:
			t.handleData(t.input[t.pos])
		case StateInSingleQuote:
			t.handleSingleQuote(t.input[t.pos])
		case StateInDoubleQuote:
			t.handleDoubleQuote(t.input[t.pos])
		case StateInLineComment:
			t.handleLineComment(t.input[t.pos])
		case StateInBlockComment:
			t.handleBlockComment(t.input[t.pos])
		}
		t.pos++
	}
	// 最後に保留中のトークンを emit する
	t.emitPendingToken()
}

// shouldContinue は、現在の読み取り位置が入力文字列の長さを超えていないかをチェックする
func (t *Tokenizer) shouldContinue() bool {
	return t.pos < len(t.input)
}

// handleData は現在の文字 (char) を見て、以下の 3つの処理 (場合によってはそのうちのいずれか) を行う
//   - 状態遷移 (e.g. handleData の実行中にシングルクォートを見つけた場合、StateInSingleQuote に遷移)
//   - トークンの確定 & 通知 (e.g. handleData で S E L E C T と読み進めた後に、 (スペース) が来た場合、SELECT トークンを確定し、OnKeyword("SELECT") を呼び出す)
//   - 文字の消費 (特にトークン確定や状態遷移が発生しない場合。 S の次に E を読む、など)
func (t *Tokenizer) handleData(char rune) {
	currentPos := t.pos

	switch char {
	case CSingleQuote:
		t.emitPendingToken()
		t.state = StateInSingleQuote
		t.start = currentPos + 1
		return

	case CDoubleQuote:
		t.emitPendingToken()
		t.state = StateInDoubleQuote
		t.start = currentPos + 1
		return

	case CDash:
		// `--` なら行コメントの開始
		if t.peekChar() == CDash {
			t.emitPendingToken()
			t.pos++ // 2つ目の '-' を読み飛ばす
			t.state = StateInLineComment
			t.start = currentPos + 2
			return
		}

	case CSlash:
		// `/*` ならブロックコメントの開始
		if t.peekChar() == CAsterisk {
			t.emitPendingToken()
			t.pos++ // '*' を読み飛ばす
			t.state = StateInBlockComment
			t.start = currentPos + 2
			return
		}

	case CSpace, CTab, CNewLine, CCarriageReturn:
		// 空白文字が来た場合、保留中のトークンを確定して通知する
		t.emitPendingToken()
		t.start = currentPos + 1
		return

	case SEqual:
		// = の次に <, >, ! が来た場合、2文字の記号として扱う (=>, =<, =!)
		if t.peekChar() == SLessThan || t.peekChar() == SGreaterThan || t.peekChar() == SExclamation {
			t.emitPendingToken()
			t.pos++ // 次の文字も消費する
			symbol := string([]rune{char, t.input[t.pos]})
			t.callbacks.onSymbol(symbol)
			t.start = currentPos + 2
			return
		}

	case SLessThan:
		// < の次に =, > が来た場合、2文字の記号として扱う (<=, <>)
		if t.peekChar() == SEqual || t.peekChar() == SGreaterThan {
			t.emitPendingToken()
			t.pos++ // 次の文字も消費する
			symbol := string([]rune{char, t.input[t.pos]})
			t.callbacks.onSymbol(symbol)
			t.start = currentPos + 2
			return
		}

	case SGreaterThan, SExclamation:
		// > の次に = が来た場合、2文字の記号として扱う (>=)
		// ! の次に = が来た場合、2文字の記号として扱う (!=)
		if t.peekChar() == SEqual {
			// >= の場合
			t.emitPendingToken()
			t.pos++ // 次の文字も消費する
			symbol := string([]rune{char, t.input[t.pos]})
			t.callbacks.onSymbol(symbol)
			t.start = currentPos + 2
			return
		}
	}
	// 上記のいずれにも当てはまらない場合、記号文字かどうかをチェックする
	if t.isSymbol(char) {
		t.emitPendingToken()
		t.callbacks.onSymbol(string(char))
		t.start = currentPos + 1
	}
}

// handleSingleQuote はシングルクォートで囲まれた文字列の終了を検出する
func (t *Tokenizer) handleSingleQuote(char rune) {
	if char == CSingleQuote {
		value := string(t.input[t.start:t.pos])
		t.callbacks.onString(value)
		t.state = StateData
		t.start = t.pos + 1
	}
}

// handleDoubleQuote はダブルクォートで囲まれた文字列の終了を検出する
func (t *Tokenizer) handleDoubleQuote(char rune) {
	if char == CDoubleQuote {
		value := string(t.input[t.start:t.pos])
		t.callbacks.onIdentifier(value)
		t.state = StateData
		t.start = t.pos + 1
	}
}

// handleLineComment は行コメントの終了を検出する
func (t *Tokenizer) handleLineComment(char rune) {
	if char == CNewLine {
		value := string(t.input[t.start:t.pos])
		t.callbacks.onComment(value)
		t.state = StateData
		t.start = t.pos + 1
	}
}

// handleBlockComment はブロックコメントの終了を検出する
func (t *Tokenizer) handleBlockComment(char rune) {
	if char == CAsterisk && t.peekChar() == CSlash {
		value := string(t.input[t.start:t.pos])
		t.callbacks.onComment(value)
		t.pos++ // 次の '/' も消費する
		t.state = StateData
		t.start = t.pos + 1
	}
}

// emitPendingToken は保留中のトークンを確定して通知する
func (t *Tokenizer) emitPendingToken() {
	// StateData 以外の場合は emit しない (未終端のクォートやコメントの内容が漏れるのを防ぐ)
	if t.state != StateData {
		return
	}
	// start と pos が同じ (空文字) なら何もしない (e.g. 空文字が2つ続いた場合など)
	if t.start >= t.pos {
		return
	}

	value := string(t.input[t.start:t.pos])

	switch {
	case t.isKeyword(value):
		t.callbacks.onKeyword(value)
	case t.isDigit(value):
		t.callbacks.onNumber(value)
	default:
		t.callbacks.onIdentifier(value)
	}
}

// isDigit は文字列が数字かどうかを判定する
func (t *Tokenizer) isDigit(word string) bool {
	for _, ch := range word {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

// isKeyword は文字列がキーワードかどうかを判定する
func (t *Tokenizer) isKeyword(word string) bool {
	keywords := []string{
		KSelect, KFrom, KWhere,
		KInsert, KInto, KValues,
		KCreate, KTable, KPrimary, KUnique, KKey,
		KDelete,
		KUpdate, KSet,
		KVarchar,
		KAnd, KOr,
		KBegin, KCommit, KRollback,
	}

	upperWord := strings.ToUpper(word)
	for _, kw := range keywords {
		if upperWord == kw {
			return true
		}
	}
	return false
}

// isSymbol は文字が記号かどうかを判定する
func (t *Tokenizer) isSymbol(ch rune) bool {
	symbols := []rune{SLeftParen, SRightParen, SComma, SSemicolon, SEqual, SLessThan, SGreaterThan, SExclamation, SAsterisk}
	for _, sym := range symbols {
		if ch == sym {
			return true
		}
	}
	return false
}

// peekChar は次の文字を覗き見る
func (t *Tokenizer) peekChar() rune {
	if t.pos+1 >= len(t.input) {
		return 0
	}
	return t.input[t.pos+1]
}
