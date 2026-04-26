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
		assert.Nil(t, p.currentParser)
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

	t.Run("FOREIGN KEY 付き CREATE TABLE 文をパースできる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		result, err := p.Parse("CREATE TABLE orders (id VARCHAR, user_id VARCHAR, PRIMARY KEY (id), KEY idx_user_id (user_id), FOREIGN KEY fk_user (user_id) REFERENCES users (id));")

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.CreateTableStmt)
		assert.True(t, ok)
		assert.Equal(t, "orders", stmt.TableName)
		assert.Equal(t, 5, len(stmt.CreateDefinitions))

		fkDef, ok := stmt.CreateDefinitions[4].(*ast.ConstraintForeignKeyDef)
		assert.True(t, ok)
		assert.Equal(t, "fk_user", fkDef.KeyName)
		assert.Equal(t, "user_id", fkDef.Column.ColName)
		assert.Equal(t, "users", fkDef.RefTable)
		assert.Equal(t, "id", fkDef.RefColumn)
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

	t.Run("START TRANSACTION 文をパースできる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		result, err := p.Parse("START TRANSACTION;")

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.TransactionStmt)
		assert.True(t, ok)
		assert.Equal(t, ast.TxBegin, stmt.Kind)
	})

	t.Run("ALTER USER 文をパースできる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		result, err := p.Parse("ALTER USER 'root'@'%' IDENTIFIED BY 'newpass';")

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.AlterUserStmt)
		assert.True(t, ok)
		assert.Equal(t, "root", stmt.Username)
		assert.Equal(t, "%", stmt.Host)
		assert.Equal(t, "newpass", stmt.Password)
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
		p.onKeyword("SELECT")

		// THEN
		assert.NotNil(t, p.currentParser)
		_, ok := p.currentParser.(*SelectParser)
		assert.True(t, ok)
	})

	t.Run("CREATE で CreateParser がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.onKeyword("CREATE")

		// THEN
		_, ok := p.currentParser.(*CreateParser)
		assert.True(t, ok)
	})

	t.Run("INSERT で InsertParser がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.onKeyword("INSERT")

		// THEN
		_, ok := p.currentParser.(*InsertParser)
		assert.True(t, ok)
	})

	t.Run("DELETE で DeleteParser がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.onKeyword("DELETE")

		// THEN
		_, ok := p.currentParser.(*DeleteParser)
		assert.True(t, ok)
	})

	t.Run("UPDATE で UpdateParser がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.onKeyword("UPDATE")

		// THEN
		_, ok := p.currentParser.(*UpdateParser)
		assert.True(t, ok)
	})

	t.Run("BEGIN で TransactionParser (TxBegin) がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.onKeyword("BEGIN")

		// THEN
		tp, ok := p.currentParser.(*TransactionParser)
		assert.True(t, ok)
		assert.Equal(t, ast.TxBegin, tp.kind)
	})

	t.Run("COMMIT で TransactionParser (TxCommit) がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.onKeyword("COMMIT")

		// THEN
		tp, ok := p.currentParser.(*TransactionParser)
		assert.True(t, ok)
		assert.Equal(t, ast.TxCommit, tp.kind)
	})

	t.Run("ROLLBACK で TransactionParser (TxRollback) がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.onKeyword("ROLLBACK")

		// THEN
		tp, ok := p.currentParser.(*TransactionParser)
		assert.True(t, ok)
		assert.Equal(t, ast.TxRollback, tp.kind)
	})

	t.Run("START で TransactionParser がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.onKeyword("START")

		// THEN
		_, ok := p.currentParser.(*TransactionParser)
		assert.True(t, ok)
	})

	t.Run("ALTER で AlterUserParser がセットされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.onKeyword("ALTER")

		// THEN
		_, ok := p.currentParser.(*AlterUserParser)
		assert.True(t, ok)
	})

	t.Run("小文字でもディスパッチされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.onKeyword("select")

		// THEN
		_, ok := p.currentParser.(*SelectParser)
		assert.True(t, ok)
	})

	t.Run("大文字小文字混合でもディスパッチされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN
		p.onKeyword("SeLeCt")

		// THEN
		_, ok := p.currentParser.(*SelectParser)
		assert.True(t, ok)
	})

	t.Run("currentHandler がある場合はデリゲートされる", func(t *testing.T) {
		// GIVEN: SELECT パース中に FROM キーワードが来る
		p := NewParser()
		p.onKeyword("SELECT")

		// WHEN
		p.onKeyword("FROM")

		// THEN: currentHandler は SelectParser のまま (上書きされない)
		_, ok := p.currentParser.(*SelectParser)
		assert.True(t, ok)
	})
}

func TestOnIdentifier(t *testing.T) {
	t.Run("currentHandler がある場合はデリゲートされる", func(t *testing.T) {
		// GIVEN: SELECT * FROM の後にテーブル名が来る
		p := NewParser()
		p.onKeyword("SELECT")
		p.onSymbol("*")
		p.onKeyword("FROM")

		// WHEN
		p.onIdentifier("users")

		// THEN: SelectParser がテーブル名を受け取っている
		result := p.currentParser.getResult()
		stmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)
		assert.Equal(t, "users", stmt.From.TableName)
	})

	t.Run("currentHandler がない場合は何も起きない", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN / THEN: パニックしない
		p.onIdentifier("users")
	})
}

func TestOnSymbol(t *testing.T) {
	t.Run("currentHandler がある場合はデリゲートされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()
		p.onKeyword("SELECT")

		// WHEN
		p.onSymbol("*")

		// THEN: SelectParser の状態が進んでいる (エラーなし)
		assert.Nil(t, p.currentParser.getError())
	})

	t.Run("currentHandler がない場合は何も起きない", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN / THEN: パニックしない
		p.onSymbol("*")
	})
}

func TestOnString(t *testing.T) {
	t.Run("currentHandler がある場合はデリゲートされる", func(t *testing.T) {
		// GIVEN: WHERE name = の後に文字列が来る
		p := NewParser()
		p.onKeyword("SELECT")
		p.onSymbol("*")
		p.onKeyword("FROM")
		p.onIdentifier("users")
		p.onKeyword("WHERE")
		p.onIdentifier("name")
		p.onSymbol("=")

		// WHEN
		p.onString("Alice")

		// THEN: エラーなく処理される
		assert.Nil(t, p.currentParser.getError())
	})

	t.Run("currentHandler がない場合は何も起きない", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN / THEN: パニックしない
		p.onString("hello")
	})
}

func TestOnNumber(t *testing.T) {
	t.Run("currentHandler がある場合はデリゲートされる", func(t *testing.T) {
		// GIVEN: WHERE id = の後に数値が来る
		p := NewParser()
		p.onKeyword("SELECT")
		p.onSymbol("*")
		p.onKeyword("FROM")
		p.onIdentifier("users")
		p.onKeyword("WHERE")
		p.onIdentifier("id")
		p.onSymbol("=")

		// WHEN
		p.onNumber("42")

		// THEN: エラーなく処理される
		assert.Nil(t, p.currentParser.getError())
	})

	t.Run("currentHandler がない場合は何も起きない", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN / THEN: パニックしない
		p.onNumber("42")
	})
}

func TestOnComment(t *testing.T) {
	t.Run("currentHandler がある場合はデリゲートされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()
		p.onKeyword("SELECT")

		// WHEN
		p.onComment("this is a comment")

		// THEN: コメントは無視され、エラーにならない
		assert.Nil(t, p.currentParser.getError())
	})

	t.Run("currentHandler がない場合は何も起きない", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN / THEN: パニックしない
		p.onComment("comment")
	})
}

func TestOnError(t *testing.T) {
	t.Run("currentHandler がある場合はエラーがデリゲートされる", func(t *testing.T) {
		// GIVEN
		p := NewParser()
		p.onKeyword("SELECT")

		// WHEN
		p.onError(errors.New("test error"))

		// THEN
		assert.Error(t, p.currentParser.getError())
		assert.Equal(t, "test error", p.currentParser.getError().Error())
	})

	t.Run("currentHandler がない場合は何も起きない", func(t *testing.T) {
		// GIVEN
		p := NewParser()

		// WHEN / THEN: パニックしない
		p.onError(errors.New("test error"))
	})
}
