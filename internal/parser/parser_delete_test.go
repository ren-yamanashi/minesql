package parser

import (
	"minesql/internal/ast"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParserDelete(t *testing.T) {
	t.Run("WHERE 句なしの DELETE 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "DELETE FROM users;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		deleteStmt, ok := result.(*ast.DeleteStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", deleteStmt.From.TableName)
		assert.Nil(t, deleteStmt.Where)
	})

	t.Run("WHERE 句付きの DELETE 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "DELETE FROM users WHERE username = 'hoge';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		deleteStmt, ok := result.(*ast.DeleteStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", deleteStmt.From.TableName)

		assert.NotNil(t, deleteStmt.Where)
		assert.NotNil(t, deleteStmt.Where.Condition)

		binaryExpr, ok := deleteStmt.Where.Condition.(*ast.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", binaryExpr.Operator)

		lhsCol, ok := binaryExpr.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "username", lhsCol.Column.ColName)

		rhsLit, ok := binaryExpr.Right.(*ast.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "hoge", rhsLit.Literal.ToString())
	})

	t.Run("不正な DELETE 文でエラーになる", func(t *testing.T) {
		t.Run("FROM 句がない場合", func(t *testing.T) {
			// GIVEN
			sql := "DELETE;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "missing FROM clause")
		})

		t.Run("テーブル名がない場合", func(t *testing.T) {
			// GIVEN
			sql := "DELETE FROM;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "missing FROM clause")
		})

		t.Run("末尾にセミコロンがない場合", func(t *testing.T) {
			// GIVEN
			sql := "DELETE FROM users"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "incomplete DELETE statement")
		})

		t.Run("不正な位置の FROM でエラーを返す", func(t *testing.T) {
			// GIVEN
			sql := "DELETE FROM users FROM;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "FROM clause is in invalid position")
		})

		t.Run("不正な位置の WHERE でエラーを返す", func(t *testing.T) {
			// GIVEN
			sql := "DELETE WHERE id = '1';"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "WHERE clause is in invalid position")
		})

		t.Run("サポートされていないキーワードでエラーを返す", func(t *testing.T) {
			// GIVEN: FROM の後に INSERT のようなサポートされていないキーワードが来る
			sql := "DELETE FROM users INSERT;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "unsupported keyword")
		})

		t.Run("不正な位置でのシンボルでエラーを返す", func(t *testing.T) {
			// GIVEN
			sql := "DELETE = FROM users;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "unexpected symbol")
		})

		t.Run("不正な位置での識別子でエラーを返す", func(t *testing.T) {
			// GIVEN
			sql := "DELETE users FROM users;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "unexpected identifier")
		})
	})
}
