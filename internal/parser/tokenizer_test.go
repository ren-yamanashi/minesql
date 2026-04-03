package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// tokenCollector はテスト用の TokenHandler 実装
type tokenCollector struct {
	keywords    []string
	identifiers []string
	numbers     []string
	strings     []string
	symbols     []string
	comments    []string
	errors      []error
}

func (c *tokenCollector) onKeyword(word string)     { c.keywords = append(c.keywords, word) }
func (c *tokenCollector) onIdentifier(ident string) { c.identifiers = append(c.identifiers, ident) }
func (c *tokenCollector) onNumber(num string)       { c.numbers = append(c.numbers, num) }
func (c *tokenCollector) onString(str string)       { c.strings = append(c.strings, str) }
func (c *tokenCollector) onSymbol(sym string)       { c.symbols = append(c.symbols, sym) }
func (c *tokenCollector) onComment(text string)     { c.comments = append(c.comments, text) }
func (c *tokenCollector) onError(err error)         { c.errors = append(c.errors, err) }

func tokenize(sql string) *tokenCollector {
	c := &tokenCollector{}
	t := NewTokenizer(sql, c)
	t.Tokenize()
	return c
}

func TestTokenizerKeywords(t *testing.T) {
	t.Run("DQL キーワードを認識する", func(t *testing.T) {
		// GIVEN
		sql := "SELECT FROM WHERE"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"SELECT", "FROM", "WHERE"}, c.keywords)
	})

	t.Run("DML キーワードを認識する", func(t *testing.T) {
		// GIVEN
		sql := "INSERT INTO VALUES DELETE UPDATE SET"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"INSERT", "INTO", "VALUES", "DELETE", "UPDATE", "SET"}, c.keywords)
	})

	t.Run("DDL キーワードを認識する", func(t *testing.T) {
		// GIVEN
		sql := "CREATE TABLE PRIMARY UNIQUE KEY VARCHAR"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"CREATE", "TABLE", "PRIMARY", "UNIQUE", "KEY", "VARCHAR"}, c.keywords)
	})

	t.Run("論理演算子キーワードを認識する", func(t *testing.T) {
		// GIVEN
		sql := "AND OR"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"AND", "OR"}, c.keywords)
	})

	t.Run("トランザクションキーワードを認識する", func(t *testing.T) {
		// GIVEN
		sql := "BEGIN COMMIT ROLLBACK"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"BEGIN", "COMMIT", "ROLLBACK"}, c.keywords)
	})

	t.Run("小文字でもキーワードとして認識する", func(t *testing.T) {
		// GIVEN
		sql := "select from where"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"select", "from", "where"}, c.keywords)
	})

	t.Run("大文字小文字混合でもキーワードとして認識する", func(t *testing.T) {
		// GIVEN
		sql := "SeLeCt FrOm"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"SeLeCt", "FrOm"}, c.keywords)
	})
}

func TestTokenizerIdentifiers(t *testing.T) {
	t.Run("テーブル名やカラム名を認識する", func(t *testing.T) {
		// GIVEN
		sql := "SELECT users_name FROM my_table"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"users_name", "my_table"}, c.identifiers)
	})

	t.Run("ダブルクォートで囲まれた識別子を認識する", func(t *testing.T) {
		// GIVEN
		sql := `SELECT "my column" FROM "my table"`

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"my column", "my table"}, c.identifiers)
	})
}

func TestTokenizerStrings(t *testing.T) {
	t.Run("シングルクォートの文字列を認識する", func(t *testing.T) {
		// GIVEN
		sql := "WHERE name = 'Alice'"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"Alice"}, c.strings)
	})

	t.Run("空文字列リテラルを認識する", func(t *testing.T) {
		// GIVEN
		sql := "WHERE name = ''"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{""}, c.strings)
	})

	t.Run("クォート内の SQL キーワードは文字列として扱われる", func(t *testing.T) {
		// GIVEN
		sql := "WHERE name = 'SELECT'"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"SELECT"}, c.strings)
		// keywords には SELECT が含まれない (WHERE のみ)
		assert.Equal(t, []string{"WHERE"}, c.keywords)
	})

	t.Run("シングルクォート内のダブルクォートはそのまま含まれる", func(t *testing.T) {
		// GIVEN
		sql := `WHERE name = 'he said "hi"'`

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{`he said "hi"`}, c.strings)
	})

	t.Run("ダブルクォート内のシングルクォートはそのまま含まれる", func(t *testing.T) {
		// GIVEN: handleDoubleQuote ではシングルクォートを特別扱いしない
		sql := `SELECT "it's" FROM t`

		// WHEN
		c := tokenize(sql)

		// THEN: "it's" 全体がダブルクォート識別子として扱われる
		assert.Contains(t, c.identifiers, "it's")
	})
}

func TestTokenizerSymbols(t *testing.T) {
	t.Run("単一文字のシンボルを認識する", func(t *testing.T) {
		// GIVEN
		sql := "( ) , ; = < > ! *"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"(", ")", ",", ";", "=", "<", ">", "!", "*"}, c.symbols)
	})

	t.Run("2 文字演算子 >=, <=, !=, <> を認識する", func(t *testing.T) {
		// GIVEN
		sql := "a >= b <= c != d <>"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Contains(t, c.symbols, ">=")
		assert.Contains(t, c.symbols, "<=")
		assert.Contains(t, c.symbols, "!=")
		assert.Contains(t, c.symbols, "<>")
	})

	t.Run("2 文字演算子 =>, =<, =! を認識する", func(t *testing.T) {
		// GIVEN
		sql := "a => b =< c =!"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Contains(t, c.symbols, "=>")
		assert.Contains(t, c.symbols, "=<")
		assert.Contains(t, c.symbols, "=!")
	})

	t.Run("末尾が演算子の場合でも正しく処理される", func(t *testing.T) {
		// GIVEN: peekChar が入力末尾に達するケース
		sql := "a >"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Contains(t, c.symbols, ">")
	})

	t.Run("シンボルに隣接するトークンを正しく分離する", func(t *testing.T) {
		// GIVEN: スペースなしで括弧やカンマが隣接
		sql := "INSERT INTO t(id,name)"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"INSERT", "INTO"}, c.keywords)
		assert.Equal(t, []string{"t", "id", "name"}, c.identifiers)
		assert.Contains(t, c.symbols, "(")
		assert.Contains(t, c.symbols, ",")
		assert.Contains(t, c.symbols, ")")
	})

	t.Run("演算子の前後にスペースがなくても分離する", func(t *testing.T) {
		// GIVEN
		sql := "WHERE id=123"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"WHERE"}, c.keywords)
		assert.Equal(t, []string{"id"}, c.identifiers)
		assert.Contains(t, c.symbols, "=")
		assert.Equal(t, []string{"123"}, c.numbers)
	})
}

func TestTokenizerComments(t *testing.T) {
	t.Run("行コメントを認識する", func(t *testing.T) {
		// GIVEN
		sql := "SELECT -- this is a comment\n* FROM users"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, 1, len(c.comments))
		assert.Equal(t, " this is a comment", c.comments[0])
	})

	t.Run("ブロックコメントを認識する", func(t *testing.T) {
		// GIVEN
		sql := "SELECT /* block comment */ * FROM users"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, 1, len(c.comments))
		assert.Equal(t, " block comment ", c.comments[0])
	})

	t.Run("改行なしの行コメントが末尾まで続く場合", func(t *testing.T) {
		// GIVEN: 改行がないまま入力が終わる
		sql := "SELECT -- no newline"

		// WHEN
		c := tokenize(sql)

		// THEN: コメントは emit されない (改行で終端されないため)
		assert.Equal(t, []string{"SELECT"}, c.keywords)
		assert.Equal(t, 0, len(c.comments))
		assert.Equal(t, 0, len(c.identifiers)) // コメント内容が識別子として漏れない
	})

	t.Run("閉じられないブロックコメントが末尾まで続く場合", func(t *testing.T) {
		// GIVEN
		sql := "SELECT /* unterminated"

		// WHEN
		c := tokenize(sql)

		// THEN: コメントは emit されない (閉じられないため)
		assert.Equal(t, []string{"SELECT"}, c.keywords)
		assert.Equal(t, 0, len(c.comments))
		assert.Equal(t, 0, len(c.identifiers)) // コメント内容が識別子として漏れない
	})
}

func TestTokenizerNumbers(t *testing.T) {
	t.Run("数値を認識する", func(t *testing.T) {
		// GIVEN
		sql := "WHERE id = 123"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"123"}, c.numbers)
	})

	t.Run("ゼロ単体を認識する", func(t *testing.T) {
		// GIVEN
		sql := "WHERE id = 0"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"0"}, c.numbers)
	})

	t.Run("数値とシンボルが隣接しても分離する", func(t *testing.T) {
		// GIVEN
		sql := "(123)"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"123"}, c.numbers)
		assert.Contains(t, c.symbols, "(")
		assert.Contains(t, c.symbols, ")")
	})
}

func TestTokenizerWhitespace(t *testing.T) {
	t.Run("タブ文字を空白として扱う", func(t *testing.T) {
		// GIVEN
		sql := "SELECT\t*\tFROM\tusers"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"SELECT", "FROM"}, c.keywords)
		assert.Equal(t, []string{"users"}, c.identifiers)
	})

	t.Run("キャリッジリターンを空白として扱う", func(t *testing.T) {
		// GIVEN
		sql := "SELECT\r*\rFROM\rusers"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"SELECT", "FROM"}, c.keywords)
	})

	t.Run("連続する空白を正しく処理する", func(t *testing.T) {
		// GIVEN
		sql := "SELECT   *   FROM   users"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"SELECT", "FROM"}, c.keywords)
		assert.Equal(t, []string{"users"}, c.identifiers)
	})
}

func TestTokenizerEdgeCases(t *testing.T) {
	t.Run("空文字列を処理してもパニックしない", func(t *testing.T) {
		// GIVEN / WHEN
		c := tokenize("")

		// THEN
		assert.Equal(t, 0, len(c.keywords))
	})

	t.Run("セミコロンをシンボルとして認識する", func(t *testing.T) {
		// GIVEN
		sql := "BEGIN;"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"BEGIN"}, c.keywords)
		assert.Contains(t, c.symbols, ";")
	})

	t.Run("閉じられないシングルクォートが末尾まで続く場合", func(t *testing.T) {
		// GIVEN
		sql := "SELECT 'unterminated"

		// WHEN
		c := tokenize(sql)

		// THEN: 文字列は emit されない (閉じられないため)
		assert.Equal(t, []string{"SELECT"}, c.keywords)
		assert.Equal(t, 0, len(c.strings))
		assert.Equal(t, 0, len(c.identifiers)) // クォート内容が識別子として漏れない
	})

	t.Run("閉じられないダブルクォートが末尾まで続く場合", func(t *testing.T) {
		// GIVEN
		sql := `SELECT "unterminated`

		// WHEN
		c := tokenize(sql)

		// THEN: 識別子は emit されない (閉じられないため)
		assert.Equal(t, []string{"SELECT"}, c.keywords)
		assert.Equal(t, 0, len(c.identifiers)) // クォート内容が識別子として漏れない
	})

	t.Run("ハイフンが単独で出現した場合は識別子として扱われる", func(t *testing.T) {
		// GIVEN: - が単独で出現 (コメントではない)
		sql := "a - b"

		// WHEN
		c := tokenize(sql)

		// THEN: - は isSymbol に含まれないため、独立した識別子として扱われる
		assert.Equal(t, []string{"a", "-", "b"}, c.identifiers)
	})

	t.Run("スラッシュが単独で出現した場合は識別子として扱われる", func(t *testing.T) {
		// GIVEN: / が単独で出現 (ブロックコメント開始ではない)
		sql := "a / b"

		// WHEN
		c := tokenize(sql)

		// THEN: / は isSymbol に含まれないため、独立した識別子として扱われる
		assert.Equal(t, []string{"a", "/", "b"}, c.identifiers)
	})
}

func TestTokenizerIntegration(t *testing.T) {
	t.Run("INSERT 文全体を正しくトークナイズする", func(t *testing.T) {
		// GIVEN
		sql := "INSERT INTO users (id, name) VALUES (1, 'Alice');"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"INSERT", "INTO", "VALUES"}, c.keywords)
		assert.Equal(t, []string{"users", "id", "name"}, c.identifiers)
		assert.Equal(t, []string{"1"}, c.numbers)
		assert.Equal(t, []string{"Alice"}, c.strings)
		assert.Equal(t, []string{"(", ",", ")", "(", ",", ")", ";"}, c.symbols)
	})

	t.Run("SELECT 文全体を正しくトークナイズする", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE name = 'Bob' AND id >= 10;"

		// WHEN
		c := tokenize(sql)

		// THEN
		assert.Equal(t, []string{"SELECT", "FROM", "WHERE", "AND"}, c.keywords)
		assert.Equal(t, []string{"users", "name", "id"}, c.identifiers)
		assert.Equal(t, []string{"Bob"}, c.strings)
		assert.Equal(t, []string{"10"}, c.numbers)
		assert.Contains(t, c.symbols, "*")
		assert.Contains(t, c.symbols, "=")
		assert.Contains(t, c.symbols, ">=")
		assert.Contains(t, c.symbols, ";")
	})
}
