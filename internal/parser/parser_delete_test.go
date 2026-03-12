package parser

import (
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/statement"
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

		deleteStmt, ok := result.(*statement.DeleteStmt)
		assert.True(t, ok)

		assert.Equal(t, statement.StmtTypeDelete, deleteStmt.StmtType)
		assert.Equal(t, "users", deleteStmt.From.TableName)
		assert.NotNil(t, deleteStmt.Where)
		assert.False(t, deleteStmt.Where.IsSet)
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

		deleteStmt, ok := result.(*statement.DeleteStmt)
		assert.True(t, ok)

		assert.Equal(t, statement.StmtTypeDelete, deleteStmt.StmtType)
		assert.Equal(t, "users", deleteStmt.From.TableName)

		assert.NotNil(t, deleteStmt.Where)
		assert.True(t, deleteStmt.Where.IsSet)
		assert.NotNil(t, deleteStmt.Where.Condition)

		binaryExpr, ok := deleteStmt.Where.Condition.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", binaryExpr.Operator)

		lhsCol, ok := binaryExpr.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "username", lhsCol.Column.ColName)

		rhsLit, ok := binaryExpr.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "hoge", rhsLit.Literal.ToString())
	})

	t.Run("WHERE 句に AND があるケースをパースできる", func(t *testing.T) {
		// GIVEN
		sql := "DELETE FROM users WHERE first_name = 'John' AND last_name = 'Doe';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		deleteStmt, ok := result.(*statement.DeleteStmt)
		assert.True(t, ok)

		assert.NotNil(t, deleteStmt.Where)
		assert.True(t, deleteStmt.Where.IsSet)

		binaryExpr, ok := deleteStmt.Where.Condition.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "AND", binaryExpr.Operator)

		// 左辺: first_name = 'John'
		leftExpr, ok := binaryExpr.Left.(*expression.LhsExpr)
		assert.True(t, ok)
		leftBinary, ok := leftExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", leftBinary.Operator)

		leftCol, ok := leftBinary.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "first_name", leftCol.Column.ColName)

		leftLit, ok := leftBinary.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "John", leftLit.Literal.ToString())

		// 右辺: last_name = 'Doe'
		rightExpr, ok := binaryExpr.Right.(*expression.RhsExpr)
		assert.True(t, ok)
		rightBinary, ok := rightExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", rightBinary.Operator)

		rightCol, ok := rightBinary.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "last_name", rightCol.Column.ColName)

		rightLit, ok := rightBinary.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "Doe", rightLit.Literal.ToString())
	})

	t.Run("比較演算子を使った WHERE 句をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "DELETE FROM users WHERE id > '10';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		deleteStmt, ok := result.(*statement.DeleteStmt)
		assert.True(t, ok)

		binaryExpr, ok := deleteStmt.Where.Condition.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, ">", binaryExpr.Operator)
	})

	t.Run("OR 演算子を使った WHERE 句をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "DELETE FROM users WHERE first_name = 'John' OR last_name = 'Doe';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		deleteStmt, ok := result.(*statement.DeleteStmt)
		assert.True(t, ok)

		binaryExpr, ok := deleteStmt.Where.Condition.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "OR", binaryExpr.Operator)

		// 左辺: first_name = 'John'
		leftExpr, ok := binaryExpr.Left.(*expression.LhsExpr)
		assert.True(t, ok)
		leftBinary, ok := leftExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", leftBinary.Operator)

		leftCol, ok := leftBinary.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "first_name", leftCol.Column.ColName)

		leftLit, ok := leftBinary.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "John", leftLit.Literal.ToString())

		// 右辺: last_name = 'Doe'
		rightExpr, ok := binaryExpr.Right.(*expression.RhsExpr)
		assert.True(t, ok)
		rightBinary, ok := rightExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", rightBinary.Operator)

		rightCol, ok := rightBinary.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "last_name", rightCol.Column.ColName)

		rightLit, ok := rightBinary.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "Doe", rightLit.Literal.ToString())
	})

	t.Run("AND と OR の優先順位が正しく処理される", func(t *testing.T) {
		// GIVEN: AND が OR より先に結合される
		// a = '1' OR b = '2' AND c = '3' → (a = '1') OR ((b = '2') AND (c = '3'))
		sql := "DELETE FROM users WHERE a = '1' OR b = '2' AND c = '3';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		deleteStmt, ok := result.(*statement.DeleteStmt)
		assert.True(t, ok)

		// ルートは OR
		orExpr, ok := deleteStmt.Where.Condition.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "OR", orExpr.Operator)

		// OR の左辺: a = '1'
		orLeftExpr, ok := orExpr.Left.(*expression.LhsExpr)
		assert.True(t, ok)
		aEqExpr, ok := orLeftExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", aEqExpr.Operator)
		aCol, ok := aEqExpr.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "a", aCol.Column.ColName)

		// OR の右辺: (b = '2') AND (c = '3')
		orRightExpr, ok := orExpr.Right.(*expression.RhsExpr)
		assert.True(t, ok)
		andExpr, ok := orRightExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "AND", andExpr.Operator)

		// AND の左辺: b = '2'
		andLeftExpr, ok := andExpr.Left.(*expression.LhsExpr)
		assert.True(t, ok)
		bEqExpr, ok := andLeftExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", bEqExpr.Operator)
		bCol, ok := bEqExpr.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "b", bCol.Column.ColName)

		// AND の右辺: c = '3'
		andRightExpr, ok := andExpr.Right.(*expression.RhsExpr)
		assert.True(t, ok)
		cEqExpr, ok := andRightExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", cEqExpr.Operator)
		cCol, ok := cEqExpr.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "c", cCol.Column.ColName)
	})

	t.Run("3 つ以上の AND 条件をパースできる", func(t *testing.T) {
		// GIVEN: 左結合で ((a = '1') AND (b = '2')) AND (c = '3') になる
		sql := "DELETE FROM users WHERE a = '1' AND b = '2' AND c = '3';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		deleteStmt, ok := result.(*statement.DeleteStmt)
		assert.True(t, ok)

		// ルートは AND (右側の AND)
		rootExpr, ok := deleteStmt.Where.Condition.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "AND", rootExpr.Operator)

		// 左辺: (a = '1') AND (b = '2')
		leftExpr, ok := rootExpr.Left.(*expression.LhsExpr)
		assert.True(t, ok)
		innerAndExpr, ok := leftExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "AND", innerAndExpr.Operator)

		// 内側 AND の左辺: a = '1'
		innerLeftExpr, ok := innerAndExpr.Left.(*expression.LhsExpr)
		assert.True(t, ok)
		aEqExpr, ok := innerLeftExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		aCol, ok := aEqExpr.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "a", aCol.Column.ColName)

		// 内側 AND の右辺: b = '2'
		innerRightExpr, ok := innerAndExpr.Right.(*expression.RhsExpr)
		assert.True(t, ok)
		bEqExpr, ok := innerRightExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		bCol, ok := bEqExpr.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "b", bCol.Column.ColName)

		// ルートの右辺: c = '3'
		rightExpr, ok := rootExpr.Right.(*expression.RhsExpr)
		assert.True(t, ok)
		cEqExpr, ok := rightExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		cCol, ok := cEqExpr.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "c", cCol.Column.ColName)
	})

	t.Run("数値リテラルを含む WHERE 句をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "DELETE FROM users WHERE id = 42;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		deleteStmt, ok := result.(*statement.DeleteStmt)
		assert.True(t, ok)

		binaryExpr, ok := deleteStmt.Where.Condition.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", binaryExpr.Operator)

		rhsLit, ok := binaryExpr.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "42", rhsLit.Literal.ToString())
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

		t.Run("WHERE 句の式が不完全な場合", func(t *testing.T) {
			// GIVEN
			sql := "DELETE FROM users WHERE id =;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "expression")
		})

		t.Run("WHERE 句が空の場合", func(t *testing.T) {
			// GIVEN
			sql := "DELETE FROM users WHERE;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "empty expression")
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
