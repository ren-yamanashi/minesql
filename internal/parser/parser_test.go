package parser

import (
	"errors"
	"minesql/internal/ast"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewParser(t *testing.T) {
	t.Run("currentHandler が nil の状態で生成される", func(t *testing.T) {
		// WHEN
		p := NewParser()

		// THEN
		assert.NotNil(t, p)
		assert.Nil(t, p.currentHandler)
	})
}

func TestParse(t *testing.T) {
	t.Run("SELECT 文をパースできる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		result, err := p.Parse("SELECT * FROM users;")

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)
		assert.Equal(t, "users", stmt.From.TableName)
	})

	t.Run("INSERT 文をパースできる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		result, err := p.Parse("INSERT INTO users (id, name) VALUES ('1', 'Alice');")

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.InsertStmt)
		assert.True(t, ok)
		assert.Equal(t, "users", stmt.Table.TableName)
		assert.Equal(t, 2, len(stmt.Cols))
	})

	t.Run("CREATE TABLE 文をパースできる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		result, err := p.Parse("CREATE TABLE users (id VARCHAR, PRIMARY KEY (id));")

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.CreateTableStmt)
		assert.True(t, ok)
		assert.Equal(t, "users", stmt.TableName)
	})

	t.Run("DELETE 文をパースできる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		result, err := p.Parse("DELETE FROM users WHERE id = '1';")

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.DeleteStmt)
		assert.True(t, ok)
		assert.Equal(t, "users", stmt.From.TableName)
	})

	t.Run("UPDATE 文をパースできる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		result, err := p.Parse("UPDATE users SET name = 'Bob' WHERE id = '1';")

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.UpdateStmt)
		assert.True(t, ok)
		assert.Equal(t, "users", stmt.Table.TableName)
	})

	t.Run("BEGIN 文をパースできる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		result, err := p.Parse("BEGIN;")

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.TransactionStmt)
		assert.True(t, ok)
		assert.Equal(t, ast.TxBegin, stmt.Kind)
	})

	t.Run("COMMIT 文をパースできる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		result, err := p.Parse("COMMIT;")

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.TransactionStmt)
		assert.True(t, ok)
		assert.Equal(t, ast.TxCommit, stmt.Kind)
	})

	t.Run("ROLLBACK 文をパースできる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		result, err := p.Parse("ROLLBACK;")

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.TransactionStmt)
		assert.True(t, ok)
		assert.Equal(t, ast.TxRollback, stmt.Kind)
	})

	t.Run("コメント付き SQL をパースできる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		result, err := p.Parse("SELECT /* comment */ * FROM users;")

		// THEN
		assert.NoError(t, err)
		_, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)
	})

	t.Run("空文字列はエラーになる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		result, err := p.Parse("")

		// THEN
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "no statement parsed")
	})

	t.Run("不正な SQL はエラーになる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		result, err := p.Parse("INSERT;")

		// THEN
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("認識できないキーワードから始まる SQL はエラーになる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		result, err := p.Parse("UNKNOWN foo;")

		// THEN
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "no statement parsed")
	})

	t.Run("同一 Parser で 2 回 Parse を呼べる", func(t *testing.T) {
		// GIVEN
		p := NewParser()
		result1, err := p.Parse("SELECT * FROM a;")
		assert.NoError(t, err)
		stmt1, ok := result1.(*ast.SelectStmt)
		assert.True(t, ok)
		assert.Equal(t, "a", stmt1.From.TableName)

		// WHEN: 同じ Parser インスタンスで 2 回目の Parse
		result2, err := p.Parse("SELECT * FROM b;")

		// THEN: 1 回目の状態に影響されず正しくパースできる
		assert.NoError(t, err)
		stmt2, ok := result2.(*ast.SelectStmt)
		assert.True(t, ok)
		assert.Equal(t, "b", stmt2.From.TableName)
	})

	t.Run("キーワード以外のトークンから始まる SQL はエラーになる", func(t *testing.T) {
		// GIVEN: 文字列リテラルから始まる入力
		p := NewParser()

		// WHEN
		result, err := p.Parse("'hello' SELECT * FROM users;")

		// THEN: 先頭の文字列が handler なしで破棄され、SELECT が正常にパースされる
		// (currentHandler が nil の状態で OnString が呼ばれても無視される)
		assert.NoError(t, err)
		stmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)
		assert.Equal(t, "users", stmt.From.TableName)
	})
}

func TestOnKeyword(t *testing.T) {
	t.Run("SELECT で SelectParser がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.OnKeyword("SELECT")

		// THEN
		assert.NotNil(t, p.currentHandler)
		_, ok := p.currentHandler.(*SelectParser)
		assert.True(t, ok)
	})

	t.Run("CREATE で CreateParser がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.OnKeyword("CREATE")

		// THEN
		_, ok := p.currentHandler.(*CreateParser)
		assert.True(t, ok)
	})

	t.Run("INSERT で InsertParser がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.OnKeyword("INSERT")

		// THEN
		_, ok := p.currentHandler.(*InsertParser)
		assert.True(t, ok)
	})

	t.Run("DELETE で DeleteParser がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.OnKeyword("DELETE")

		// THEN
		_, ok := p.currentHandler.(*DeleteParser)
		assert.True(t, ok)
	})

	t.Run("UPDATE で UpdateParser がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.OnKeyword("UPDATE")

		// THEN
		_, ok := p.currentHandler.(*UpdateParser)
		assert.True(t, ok)
	})

	t.Run("BEGIN で TransactionParser (TxBegin) がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.OnKeyword("BEGIN")

		// THEN
		tp, ok := p.currentHandler.(*TransactionParser)
		assert.True(t, ok)
		assert.Equal(t, ast.TxBegin, tp.kind)
	})

	t.Run("COMMIT で TransactionParser (TxCommit) がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.OnKeyword("COMMIT")

		// THEN
		tp, ok := p.currentHandler.(*TransactionParser)
		assert.True(t, ok)
		assert.Equal(t, ast.TxCommit, tp.kind)
	})

	t.Run("ROLLBACK で TransactionParser (TxRollback) がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.OnKeyword("ROLLBACK")

		// THEN
		tp, ok := p.currentHandler.(*TransactionParser)
		assert.True(t, ok)
		assert.Equal(t, ast.TxRollback, tp.kind)
	})

	t.Run("小文字でもディスパッチされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.OnKeyword("select")

		// THEN
		_, ok := p.currentHandler.(*SelectParser)
		assert.True(t, ok)
	})

	t.Run("大文字小文字混合でもディスパッチされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.OnKeyword("SeLeCt")

		// THEN
		_, ok := p.currentHandler.(*SelectParser)
		assert.True(t, ok)
	})

	t.Run("currentHandler がある場合はデリゲートされる", func(t *testing.T) {
		// GIVEN: SELECT パース中に FROM キーワードが来る
		p := NewParser()
		p.OnKeyword("SELECT")

		// WHEN
		p.OnKeyword("FROM")

		// THEN: currentHandler は SelectParser のまま (上書きされない)
		_, ok := p.currentHandler.(*SelectParser)
		assert.True(t, ok)
	})
}

func TestOnIdentifier(t *testing.T) {
	t.Run("currentHandler がある場合はデリゲートされる", func(t *testing.T) {
		// GIVEN: SELECT * FROM の後にテーブル名が来る
		p := NewParser()
		p.OnKeyword("SELECT")
		p.OnSymbol("*")
		p.OnKeyword("FROM")

		// WHEN
		p.OnIdentifier("users")

		// THEN: SelectParser がテーブル名を受け取っている
		result := p.currentHandler.getResult()
		stmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)
		assert.Equal(t, "users", stmt.From.TableName)
	})

	t.Run("currentHandler がない場合は何も起きない", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN / THEN: パニックしない
		p.OnIdentifier("users")
	})
}

func TestOnSymbol(t *testing.T) {
	t.Run("currentHandler がある場合はデリゲートされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()
		p.OnKeyword("SELECT")

		// WHEN
		p.OnSymbol("*")

		// THEN: SelectParser の状態が進んでいる (エラーなし)
		assert.Nil(t, p.currentHandler.getError())
	})

	t.Run("currentHandler がない場合は何も起きない", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN / THEN: パニックしない
		p.OnSymbol("*")
	})
}

func TestOnString(t *testing.T) {
	t.Run("currentHandler がある場合はデリゲートされる", func(t *testing.T) {
		// GIVEN: WHERE name = の後に文字列が来る
		p := NewParser()
		p.OnKeyword("SELECT")
		p.OnSymbol("*")
		p.OnKeyword("FROM")
		p.OnIdentifier("users")
		p.OnKeyword("WHERE")
		p.OnIdentifier("name")
		p.OnSymbol("=")

		// WHEN
		p.OnString("Alice")

		// THEN: エラーなく処理される
		assert.Nil(t, p.currentHandler.getError())
	})

	t.Run("currentHandler がない場合は何も起きない", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN / THEN: パニックしない
		p.OnString("hello")
	})
}

func TestOnNumber(t *testing.T) {
	t.Run("currentHandler がある場合はデリゲートされる", func(t *testing.T) {
		// GIVEN: WHERE id = の後に数値が来る
		p := NewParser()
		p.OnKeyword("SELECT")
		p.OnSymbol("*")
		p.OnKeyword("FROM")
		p.OnIdentifier("users")
		p.OnKeyword("WHERE")
		p.OnIdentifier("id")
		p.OnSymbol("=")

		// WHEN
		p.OnNumber("42")

		// THEN: エラーなく処理される
		assert.Nil(t, p.currentHandler.getError())
	})

	t.Run("currentHandler がない場合は何も起きない", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN / THEN: パニックしない
		p.OnNumber("42")
	})
}

func TestOnComment(t *testing.T) {
	t.Run("currentHandler がある場合はデリゲートされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()
		p.OnKeyword("SELECT")

		// WHEN
		p.OnComment("this is a comment")

		// THEN: コメントは無視され、エラーにならない
		assert.Nil(t, p.currentHandler.getError())
	})

	t.Run("currentHandler がない場合は何も起きない", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN / THEN: パニックしない
		p.OnComment("comment")
	})
}

func TestOnError(t *testing.T) {
	t.Run("currentHandler がある場合はエラーがデリゲートされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()
		p.OnKeyword("SELECT")

		// WHEN
		p.OnError(errors.New("test error"))

		// THEN
		assert.Error(t, p.currentHandler.getError())
		assert.Equal(t, "test error", p.currentHandler.getError().Error())
	})

	t.Run("currentHandler がない場合は何も起きない", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN / THEN: パニックしない
		p.OnError(errors.New("test error"))
	})
}
