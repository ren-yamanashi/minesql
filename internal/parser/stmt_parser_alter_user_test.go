package parser

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/ast"
	"github.com/stretchr/testify/assert"
)

func TestParserAlterUser(t *testing.T) {
	t.Run("ALTER USER 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "ALTER USER 'root'@'%' IDENTIFIED BY 'newpass';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		stmt, ok := result.(*ast.AlterUserStmt)
		assert.True(t, ok)
		assert.Equal(t, "root", stmt.Username)
		assert.Equal(t, "%", stmt.Host)
		assert.Equal(t, "newpass", stmt.Password)
	})

	t.Run("ホスト付きの ALTER USER 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "ALTER USER 'admin'@'192.168.1.%' IDENTIFIED BY 'secret123';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.AlterUserStmt)
		assert.True(t, ok)
		assert.Equal(t, "admin", stmt.Username)
		assert.Equal(t, "192.168.1.%", stmt.Host)
		assert.Equal(t, "secret123", stmt.Password)
	})

	t.Run("セミコロンなしでもパースできる", func(t *testing.T) {
		// GIVEN: mysql クライアントはセミコロンを除去して送信する
		sql := "ALTER USER 'root'@'%' IDENTIFIED BY 'newpass'"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.AlterUserStmt)
		assert.True(t, ok)
		assert.Equal(t, "root", stmt.Username)
		assert.Equal(t, "newpass", stmt.Password)
	})

	t.Run("不正な ALTER USER 文でエラーになる", func(t *testing.T) {
		t.Run("ALTER の後に USER 以外が来た場合", func(t *testing.T) {
			// GIVEN
			sql := "ALTER TABLE users;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "expected USER after ALTER")
		})

		t.Run("ユーザー名がない場合", func(t *testing.T) {
			// GIVEN
			sql := "ALTER USER IDENTIFIED BY 'newpass';"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("@ がない場合", func(t *testing.T) {
			// GIVEN
			sql := "ALTER USER 'root' IDENTIFIED BY 'newpass';"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("IDENTIFIED BY がない場合", func(t *testing.T) {
			// GIVEN
			sql := "ALTER USER 'root'@'%';"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "unexpected symbol")
		})

		t.Run("BY の後にパスワードがない場合", func(t *testing.T) {
			// GIVEN
			sql := "ALTER USER 'root'@'%' IDENTIFIED BY;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
		})
	})
}
