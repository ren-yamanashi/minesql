package parser

import "strings"

// State
type State int

const (
	StateData           State = iota
	StateInSingleQuote        // '...'
	StateInDoubleQuote        // "..."
	StateInLineComment        // -- ...
	StateInBlockComment       // /* ... */
)

// CharCodes
const (
	CharCodeSingleQuote    rune = '\''
	CharCodeDoubleQuote    rune = '"'
	CharCodeDash           rune = '-'
	CharCodeSlash          rune = '/'
	CharCodeAsterisk       rune = '*'
	CharCodeSpace          rune = ' '
	CharCodeTab            rune = '\t'
	CharCodeNewLine        rune = '\n'
	CharCodeCarriageReturn rune = '\r'
)

type TokenHandler interface {
	OnKeyword(word string)     // SELECT, FROM, WHERE ...
	OnIdentifier(ident string) // users, id ...
	OnString(value string)     // 'hello'
	OnNumber(num string)       // 123
	OnSymbol(symbol string)    // (, ), =, >=
	OnComment(text string)     // -- ...
	OnError(err error)
}

type Tokenizer struct {
	// イベントの通知先
	callbacks TokenHandler
	// 現在のステート
	state State
	// 解析対象の入力文字列
	input []rune
	// 現在の読み取り位置
	pos int
	// 現在のトークン開始位置
	start int
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

func (t *Tokenizer) Write(chunk []rune) {
	t.pos += len(chunk)
	t.input = append(t.input, chunk...)
}

// input を走査し、現在の状態に対応する関数を呼び出す
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
}

func (t *Tokenizer) shouldContinue() bool {
	return t.pos < len(t.input)
}

// 現在の文字 (char) を見て、以下の 3つの処理 (場合によってはそのうちのいずれか) を行う
// - 状態遷移 (e.g. handleData の実行中にシングルクォートを見つけた場合、StateInSingleQuote に遷移)
// - トークンの確定 & 通知 (e.g. handleData で S E L E C T と読み進めた後に、 (スペース) が来た場合、SELECT トークンを確定し、OnKeyword("SELECT") を呼び出す)
// - 文字の消費 (特にトークン確定や状態遷移が発生しない場合。 S の次に E を読む、など)
func (t *Tokenizer) handleData(char rune) {
	currentPos := t.pos

	switch char {
	case CharCodeSingleQuote:
		t.emitPendingToken()
		t.state = StateInSingleQuote
		t.start = currentPos + 1
		return
	case CharCodeDoubleQuote:
		t.emitPendingToken()
		t.state = StateInDoubleQuote
		t.start = currentPos + 1
		return
	case CharCodeDash:
		if t.peekChar() == CharCodeDash {
			t.emitPendingToken()
			t.pos++ // 2つ目の '-' を読み飛ばす
			t.state = StateInLineComment
			t.start = currentPos + 2
			return
		} else {
			t.emitPendingToken()
			t.callbacks.OnSymbol(string(char)) // マイナス記号として通知
			t.start = currentPos + 1
			return
		}
	case CharCodeSlash:
		if t.peekChar() == CharCodeAsterisk {
			t.emitPendingToken()
			t.pos++ // '*' を読み飛ばす
			t.state = StateInBlockComment
			t.start = currentPos + 2
			return
		} else {
			t.emitPendingToken()
			t.callbacks.OnSymbol(string(char)) // スラッシュ記号として通知
			t.start = currentPos + 1
			return
		}
	case CharCodeSpace, CharCodeTab, CharCodeNewLine, CharCodeCarriageReturn:
		t.emitPendingToken()
		t.start = currentPos + 1
		return
	default:
		if t.isSymbol(char) {
			t.emitPendingToken()
			t.callbacks.OnSymbol(string(char))
			t.start = currentPos + 1
		}
		return
	}
}

// シングルクォートで囲まれた文字列の終了を検出する
func (t *Tokenizer) handleSingleQuote(char rune) {
	if char == CharCodeSingleQuote {
		value := string(t.input[t.start:t.pos])
		t.callbacks.OnString(value)
		t.state = StateData
		t.start = t.pos + 1
	}
}

// ダブルクォートの終了を検出する
func (t *Tokenizer) handleDoubleQuote(char rune) {
	if char == CharCodeDoubleQuote {
		value := string(t.input[t.start:t.pos])
		t.callbacks.OnIdentifier(value)
		t.state = StateData
		t.start = t.pos + 1
	}
}

// 行コメントの終了を検出する
func (t *Tokenizer) handleLineComment(char rune) {
	if char == CharCodeNewLine {
		value := string(t.input[t.start:t.pos])
		t.callbacks.OnComment(value)
		t.state = StateData
		t.start = t.pos + 1
	}
}

// ブロックコメントの終了を検出する
func (t *Tokenizer) handleBlockComment(char rune) {
	if char == CharCodeAsterisk && t.peekChar() == CharCodeSlash {
		value := string(t.input[t.start:t.pos])
		t.callbacks.OnComment(value)
		t.pos++ // 次の '/' も消費する
		t.state = StateData
		t.start = t.pos + 1
	}
}

// 保留中のトークンを確定して通知する
func (t *Tokenizer) emitPendingToken() {
	// start と pos が同じ (空文字) なら何もしない (e.g. 空文字が2つ続いた場合など)
	if t.start >= t.pos {
		return
	}

	value := string(t.input[t.start:t.pos])

	if t.isKeyword(value) {
		t.callbacks.OnKeyword(value)
	} else if t.isDigit(value) {
		t.callbacks.OnNumber(value)
	} else {
		t.callbacks.OnIdentifier(value)
	}
}

// 数字かどうか
func (t *Tokenizer) isDigit(word string) bool {
	for _, ch := range word {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

// キーワードかどうか
func (t *Tokenizer) isKeyword(word string) bool {
	keywords := []string{
		"SELECT", "FROM", "WHERE",
		"INSERT", "INTO", "VALUES",
		"CREATE", "TABLE", "PRIMARY", "UNIQUE", "KEY",
		"VARCHAR",
		"AND",
	}

	upperWord := strings.ToUpper(word)
	for _, kw := range keywords {
		if upperWord == kw {
			return true
		}
	}
	return false
}

// 記号文字かどうか
func (t *Tokenizer) isSymbol(ch rune) bool {
	symbols := []rune{'(', ')', ',', ';', '=', '<', '>', '!', '*'}
	for _, sym := range symbols {
		if ch == sym {
			return true
		}
	}
	return false
}

// 次の文字を覗き見る
func (t *Tokenizer) peekChar() rune {
	if t.pos+1 >= len(t.input) {
		return 0
	}
	return t.input[t.pos+1]
}
