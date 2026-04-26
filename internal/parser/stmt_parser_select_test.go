package parser

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/ast"
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

		t.Run("SELECT で特定カラム名を指定した場合にパースできる", func(t *testing.T) {
			// GIVEN
			sql := "SELECT id FROM users;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.NoError(t, err)
			assert.NotNil(t, result)
			selectStmt, ok := result.(*ast.SelectStmt)
			assert.True(t, ok)
			assert.Len(t, selectStmt.Columns, 1)
			assert.Equal(t, "id", selectStmt.Columns[0].ColName)
			assert.Equal(t, "users", selectStmt.From.TableName)
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

	t.Run("SELECT で複数カラムを指定できる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT id, name, age FROM users;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)
		assert.Len(t, selectStmt.Columns, 3)
		assert.Equal(t, "id", selectStmt.Columns[0].ColName)
		assert.Equal(t, "name", selectStmt.Columns[1].ColName)
		assert.Equal(t, "age", selectStmt.Columns[2].ColName)
	})

	t.Run("SELECT で修飾名カラムを指定できる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT users.id, users.name FROM users;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)
		assert.Len(t, selectStmt.Columns, 2)
		assert.Equal(t, "users", selectStmt.Columns[0].TableName)
		assert.Equal(t, "id", selectStmt.Columns[0].ColName)
		assert.Equal(t, "users", selectStmt.Columns[1].TableName)
		assert.Equal(t, "name", selectStmt.Columns[1].ColName)
	})

	t.Run("SELECT * の場合 Columns は nil", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)
		assert.Nil(t, selectStmt.Columns)
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

	t.Run("INNER JOIN 付きの SELECT 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", selectStmt.From.TableName)

		assert.Len(t, selectStmt.Joins, 1)
		join := selectStmt.Joins[0]
		assert.Equal(t, "orders", join.Table.TableName)

		assert.NotNil(t, join.Condition)
		assert.Equal(t, "=", join.Condition.Operator)

		lhsCol, ok := join.Condition.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "users", lhsCol.Column.TableName)
		assert.Equal(t, "id", lhsCol.Column.ColName)

		rhsCol, ok := join.Condition.Right.(*ast.RhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "orders", rhsCol.Column.TableName)
		assert.Equal(t, "user_id", rhsCol.Column.ColName)
	})

	t.Run("JOIN (INNER 省略) 付きの SELECT 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users JOIN orders ON users.id = orders.user_id;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", selectStmt.From.TableName)
		assert.Len(t, selectStmt.Joins, 1)

		join := selectStmt.Joins[0]
		assert.Equal(t, "orders", join.Table.TableName)
		assert.Equal(t, "=", join.Condition.Operator)
	})

	t.Run("JOIN + WHERE 付きの SELECT 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = '1';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", selectStmt.From.TableName)
		assert.Len(t, selectStmt.Joins, 1)

		join := selectStmt.Joins[0]
		assert.Equal(t, "orders", join.Table.TableName)

		assert.NotNil(t, selectStmt.Where)
		whereExpr := selectStmt.Where.Condition
		assert.Equal(t, "=", whereExpr.Operator)

		lhsCol, ok := whereExpr.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "users", lhsCol.Column.TableName)
		assert.Equal(t, "id", lhsCol.Column.ColName)
	})

	t.Run("複数の JOIN をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN items ON orders.item_id = items.id;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", selectStmt.From.TableName)
		assert.Len(t, selectStmt.Joins, 2)

		assert.Equal(t, "orders", selectStmt.Joins[0].Table.TableName)
		assert.Equal(t, "=", selectStmt.Joins[0].Condition.Operator)

		assert.Equal(t, "items", selectStmt.Joins[1].Table.TableName)
		assert.Equal(t, "=", selectStmt.Joins[1].Condition.Operator)
	})

	t.Run("ON 条件で AND を使った複合条件をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users JOIN orders ON users.id = orders.user_id AND users.name = orders.name;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		assert.Len(t, selectStmt.Joins, 1)
		join := selectStmt.Joins[0]
		assert.Equal(t, "AND", join.Condition.Operator)
	})

	t.Run("不正な JOIN 文でエラーになる", func(t *testing.T) {
		t.Run("ON 句がない場合", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * FROM users JOIN orders;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "ON")
		})

		t.Run("JOIN のテーブル名がない場合", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * FROM users JOIN ON users.id = orders.user_id;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "invalid position")
		})

		t.Run("ON 条件が空の場合", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * FROM users JOIN orders ON;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "expression")
		})

		t.Run("INNER の後に JOIN がない場合", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * FROM users INNER orders ON users.id = orders.user_id;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("FROM 句の前に JOIN がある場合", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * JOIN orders ON users.id = orders.user_id;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
		})
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
