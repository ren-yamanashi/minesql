package parser

import (
	"minesql/internal/ast"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParserUpdate(t *testing.T) {
	t.Run("WHERE 句なしの単一カラム UPDATE 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "UPDATE users SET first_name = 'Jane';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		updateStmt, ok := result.(*ast.UpdateStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", updateStmt.Table.TableName)
		assert.Equal(t, 1, len(updateStmt.SetClauses))
		assert.Equal(t, "first_name", updateStmt.SetClauses[0].Column.ColName)
		assert.Equal(t, "Jane", updateStmt.SetClauses[0].Value.ToString())
		assert.Nil(t, updateStmt.Where)
	})

	t.Run("複数カラムの UPDATE 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "UPDATE users SET first_name = 'Jane', last_name = 'Doe';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		updateStmt, ok := result.(*ast.UpdateStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", updateStmt.Table.TableName)
		assert.Equal(t, 2, len(updateStmt.SetClauses))
		assert.Equal(t, "first_name", updateStmt.SetClauses[0].Column.ColName)
		assert.Equal(t, "Jane", updateStmt.SetClauses[0].Value.ToString())
		assert.Equal(t, "last_name", updateStmt.SetClauses[1].Column.ColName)
		assert.Equal(t, "Doe", updateStmt.SetClauses[1].Value.ToString())
	})

	t.Run("WHERE 句付きの UPDATE 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "UPDATE users SET first_name = 'Jane' WHERE id = '1';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		updateStmt, ok := result.(*ast.UpdateStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", updateStmt.Table.TableName)
		assert.Equal(t, 1, len(updateStmt.SetClauses))
		assert.Equal(t, "first_name", updateStmt.SetClauses[0].Column.ColName)
		assert.Equal(t, "Jane", updateStmt.SetClauses[0].Value.ToString())

		assert.NotNil(t, updateStmt.Where)

		binaryExpr := updateStmt.Where.Condition
		assert.Equal(t, "=", binaryExpr.Operator)

		lhsCol, ok := binaryExpr.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "id", lhsCol.Column.ColName)

		rhsLit, ok := binaryExpr.Right.(*ast.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "1", rhsLit.Literal.ToString())
	})

	t.Run("複数カラム更新 + WHERE 句付きの UPDATE 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "UPDATE users SET first_name = 'Jane', last_name = 'Smith' WHERE id = '1' AND gender = 'female';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		updateStmt, ok := result.(*ast.UpdateStmt)
		assert.True(t, ok)

		assert.Equal(t, 2, len(updateStmt.SetClauses))
		assert.Equal(t, "first_name", updateStmt.SetClauses[0].Column.ColName)
		assert.Equal(t, "last_name", updateStmt.SetClauses[1].Column.ColName)

		assert.NotNil(t, updateStmt.Where)
		assert.NotNil(t, updateStmt.Where.Condition)

		// AND で結合された式
		andExpr := updateStmt.Where.Condition
		assert.Equal(t, "AND", andExpr.Operator)
	})

	t.Run("数値リテラルを SET 句の値に使用できる", func(t *testing.T) {
		// GIVEN
		sql := "UPDATE users SET age = 30;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		updateStmt, ok := result.(*ast.UpdateStmt)
		assert.True(t, ok)

		assert.Equal(t, 1, len(updateStmt.SetClauses))
		assert.Equal(t, "age", updateStmt.SetClauses[0].Column.ColName)
		assert.Equal(t, "30", updateStmt.SetClauses[0].Value.ToString())
	})

	t.Run("WHERE 句で OR 演算子を使った UPDATE 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "UPDATE users SET first_name = 'Jane' WHERE id = '1' OR id = '2';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		updateStmt, ok := result.(*ast.UpdateStmt)
		assert.True(t, ok)

		assert.NotNil(t, updateStmt.Where)

		orExpr := updateStmt.Where.Condition
		assert.Equal(t, "OR", orExpr.Operator)
	})

	t.Run("WHERE 句で AND と OR の優先順位が正しく処理される", func(t *testing.T) {
		// GIVEN
		sql := "UPDATE users SET first_name = 'Jane' WHERE a = '1' OR b = '2' AND c = '3';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN: AND が先に結合され、(a = '1') OR ((b = '2') AND (c = '3')) になる
		assert.NoError(t, err)
		assert.NotNil(t, result)

		updateStmt, ok := result.(*ast.UpdateStmt)
		assert.True(t, ok)

		// ルートは OR
		orExpr := updateStmt.Where.Condition
		assert.Equal(t, "OR", orExpr.Operator)

		// OR の左辺は a = '1'
		leftExpr, ok := orExpr.Left.(*ast.LhsExpr)
		assert.True(t, ok)
		leftBinary := leftExpr.Expr
		assert.Equal(t, "=", leftBinary.Operator)

		// OR の右辺は AND 式
		rightExpr, ok := orExpr.Right.(*ast.RhsExpr)
		assert.True(t, ok)
		rightBinary := rightExpr.Expr
		assert.Equal(t, "AND", rightBinary.Operator)
	})

	t.Run("WHERE 句で数値リテラルを使用できる", func(t *testing.T) {
		// GIVEN
		sql := "UPDATE users SET first_name = 'Jane' WHERE age = 30;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		updateStmt, ok := result.(*ast.UpdateStmt)
		assert.True(t, ok)

		assert.NotNil(t, updateStmt.Where)

		binaryExpr := updateStmt.Where.Condition

		rhsLit, ok := binaryExpr.Right.(*ast.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "30", rhsLit.Literal.ToString())
	})

	t.Run("不正な UPDATE 文でエラーになる", func(t *testing.T) {
		t.Run("SET 句がない場合", func(t *testing.T) {
			// GIVEN
			sql := "UPDATE users;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "missing SET clause")
		})

		t.Run("テーブル名がない場合", func(t *testing.T) {
			// GIVEN
			sql := "UPDATE SET first_name = 'Jane';"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "SET clause is in invalid position")
		})

		t.Run("末尾にセミコロンがない場合", func(t *testing.T) {
			// GIVEN
			sql := "UPDATE users SET first_name = 'Jane'"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "incomplete UPDATE statement")
		})

		t.Run("SET 句のカラム名の後に = がない場合", func(t *testing.T) {
			// GIVEN
			sql := "UPDATE users SET first_name 'Jane';"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "unexpected string")
		})

		t.Run("SET 句の値がない場合", func(t *testing.T) {
			// GIVEN
			sql := "UPDATE users SET first_name =;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "missing SET clause")
		})

		t.Run("不正な位置の WHERE でエラーを返す", func(t *testing.T) {
			// GIVEN
			sql := "UPDATE users WHERE id = '1';"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "WHERE clause is in invalid position")
		})

		t.Run("サポートされていないキーワードでエラーを返す", func(t *testing.T) {
			// GIVEN
			sql := "UPDATE users SET first_name = 'Jane' INSERT;"
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
			sql := "UPDATE = users SET first_name = 'Jane';"
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
			sql := "UPDATE users users SET first_name = 'Jane';"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "unexpected identifier")
		})

		t.Run("SET 句のカラム名の後に = 以外のシンボルが来た場合", func(t *testing.T) {
			// GIVEN
			sql := "UPDATE users SET first_name > 'Jane';"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "expected '=' after column name in SET clause")
		})

		t.Run("SET 句の値の後に不正なシンボルが来た場合", func(t *testing.T) {
			// GIVEN
			sql := "UPDATE users SET first_name = 'Jane' > last_name = 'Doe';"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "unexpected symbol in SET clause")
		})

		t.Run("AND/OR が WHERE 以外の状態で来た場合", func(t *testing.T) {
			// GIVEN
			sql := "UPDATE users AND SET first_name = 'Jane';"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "operator is in invalid position")
		})

		t.Run("不正な状態で数値が来た場合", func(t *testing.T) {
			// GIVEN
			sql := "UPDATE 42 SET first_name = 'Jane';"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "unexpected number")
		})

		t.Run("UPDATE の後に直接セミコロンが来た場合", func(t *testing.T) {
			// GIVEN: テーブル名なしで即座に文が終了
			sql := "UPDATE;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "missing table name")
		})
	})
}
