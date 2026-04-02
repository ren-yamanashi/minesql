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
	})
}
