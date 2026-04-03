package parser

import (
	"minesql/internal/ast"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParserSelect(t *testing.T) {
	t.Run("WHERE 句なしの SELECT 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", selectStmt.From.TableName)
		assert.Nil(t, selectStmt.Where)
	})

	t.Run("WHERE 句付きの SELECT 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id = '1';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", selectStmt.From.TableName)

		assert.NotNil(t, selectStmt.Where)
		assert.NotNil(t, selectStmt.Where.Condition)

		binaryExpr := selectStmt.Where.Condition
		assert.Equal(t, "=", binaryExpr.Operator)

		lhsCol, ok := binaryExpr.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "id", lhsCol.Column.ColName)

		rhsLit, ok := binaryExpr.Right.(*ast.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "1", rhsLit.Literal.ToString())
	})

	t.Run("コメント付きの SELECT 文をパースできる", func(t *testing.T) {
		t.Run("行コメント付き", func(t *testing.T) {
			// GIVEN
			sql := `
-- これはコメント
SELECT * FROM users -- テーブル users を選択
WHERE id = '1' -- id が 1 のレコード
;
`
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.NoError(t, err)
			assert.NotNil(t, result)

			selectStmt, ok := result.(*ast.SelectStmt)
			assert.True(t, ok)
			assert.Equal(t, "users", selectStmt.From.TableName)
			assert.NotNil(t, selectStmt.Where)
		})

		t.Run("ブロックコメント付き", func(t *testing.T) {
			// GIVEN
			sql := `
/* これはコメント */
SELECT * FROM users /* テーブル users を選択 */
WHERE id = '1' /* id が 1 のレコード */
;
`
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.NoError(t, err)
			assert.NotNil(t, result)

			selectStmt, ok := result.(*ast.SelectStmt)
			assert.True(t, ok)
			assert.Equal(t, "users", selectStmt.From.TableName)
			assert.NotNil(t, selectStmt.Where)
		})
	})

	t.Run("不正な SELECT 文でエラーになる", func(t *testing.T) {
		t.Run("FROM 句がない場合", func(t *testing.T) {
			// GIVEN
			sql := "SELECT *;"
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
			sql := "SELECT * FROM;"
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
			sql := "SELECT * FROM users"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "incomplete SELECT statement")
		})

		t.Run("SELECT で特定カラム名を指定した場合エラーになる", func(t *testing.T) {
			// GIVEN
			sql := "SELECT id FROM users;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "currently only SELECT * is supported")
		})

		t.Run("FROM 句が重複している場合エラーになる", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * FROM users FROM orders;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "FROM clause is in invalid position")
		})

		t.Run("FROM 句なしで WHERE 句がある場合エラーになる", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * WHERE id = '1';"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "WHERE clause is in invalid position")
		})

		t.Run("サポートされていないキーワードがある場合エラーになる", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * FROM users SET name = 'john';"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "unsupported keyword")
		})

		t.Run("WHERE 句の条件式がない場合エラーになる", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * FROM users WHERE;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "empty expression in WHERE clause")
		})
	})

	t.Run("WHERE 句で数値リテラルを使用できる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE age = 25;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		assert.NotNil(t, selectStmt.Where)
		binaryExpr := selectStmt.Where.Condition
		assert.Equal(t, "=", binaryExpr.Operator)

		rhsLit, ok := binaryExpr.Right.(*ast.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "25", rhsLit.Literal.ToString())
	})

	t.Run("WHERE 句で AND を使った複合条件をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id = '1' AND name = 'john';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		assert.NotNil(t, selectStmt.Where)
		binaryExpr := selectStmt.Where.Condition
		assert.Equal(t, "AND", binaryExpr.Operator)

		lhsExpr, ok := binaryExpr.Left.(*ast.LhsExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", lhsExpr.Expr.Operator)

		rhsExpr, ok := binaryExpr.Right.(*ast.RhsExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", rhsExpr.Expr.Operator)
	})
}
