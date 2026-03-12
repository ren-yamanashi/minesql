package parser

import (
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/statement"
	"testing"

	"github.com/stretchr/testify/assert"
)

// WHERE 句のパースロジックは WhereParser で共通化されているため、
// SELECT 文を経由して WHERE 句のパースを網羅的にテストする。

func TestParserWhere(t *testing.T) {
	t.Run("AND 演算子を使った WHERE 句をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id = '1' AND name = 'John';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*statement.SelectStmt)
		assert.True(t, ok)

		assert.NotNil(t, selectStmt.Where)
		assert.True(t, selectStmt.Where.IsSet)

		binaryExpr, ok := selectStmt.Where.Condition.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "AND", binaryExpr.Operator)

		// 左辺: id = '1'
		leftExpr, ok := binaryExpr.Left.(*expression.LhsExpr)
		assert.True(t, ok)
		leftBinary, ok := leftExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", leftBinary.Operator)

		leftCol, ok := leftBinary.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "id", leftCol.Column.ColName)

		leftLit, ok := leftBinary.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "1", leftLit.Literal.ToString())

		// 右辺: name = 'John'
		rightExpr, ok := binaryExpr.Right.(*expression.RhsExpr)
		assert.True(t, ok)
		rightBinary, ok := rightExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", rightBinary.Operator)

		rightCol, ok := rightBinary.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "name", rightCol.Column.ColName)

		rightLit, ok := rightBinary.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "John", rightLit.Literal.ToString())
	})

	t.Run("OR 演算子を使った WHERE 句をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id = '1' OR id = '2';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*statement.SelectStmt)
		assert.True(t, ok)

		binaryExpr, ok := selectStmt.Where.Condition.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "OR", binaryExpr.Operator)

		// 左辺: id = '1'
		leftExpr, ok := binaryExpr.Left.(*expression.LhsExpr)
		assert.True(t, ok)
		leftBinary, ok := leftExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", leftBinary.Operator)

		leftCol, ok := leftBinary.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "id", leftCol.Column.ColName)

		leftLit, ok := leftBinary.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "1", leftLit.Literal.ToString())

		// 右辺: id = '2'
		rightExpr, ok := binaryExpr.Right.(*expression.RhsExpr)
		assert.True(t, ok)
		rightBinary, ok := rightExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", rightBinary.Operator)

		rightCol, ok := rightBinary.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "id", rightCol.Column.ColName)

		rightLit, ok := rightBinary.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "2", rightLit.Literal.ToString())
	})

	t.Run("AND と OR の優先順位が正しく処理される", func(t *testing.T) {
		// GIVEN: AND が OR より先に結合される
		// a = '1' OR b = '2' AND c = '3' → (a = '1') OR ((b = '2') AND (c = '3'))
		sql := "SELECT * FROM users WHERE a = '1' OR b = '2' AND c = '3';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*statement.SelectStmt)
		assert.True(t, ok)

		// ルートは OR
		orExpr, ok := selectStmt.Where.Condition.(*expression.BinaryExpr)
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
		aLit, ok := aEqExpr.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "1", aLit.Literal.ToString())

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
		sql := "SELECT * FROM users WHERE a = '1' AND b = '2' AND c = '3';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*statement.SelectStmt)
		assert.True(t, ok)

		// ルートは AND (右側の AND)
		rootExpr, ok := selectStmt.Where.Condition.(*expression.BinaryExpr)
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

	t.Run("比較演算子を使った WHERE 句をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id > '10';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*statement.SelectStmt)
		assert.True(t, ok)

		binaryExpr, ok := selectStmt.Where.Condition.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, ">", binaryExpr.Operator)
	})

	t.Run("数値リテラルを含む WHERE 句をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id = 42;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*statement.SelectStmt)
		assert.True(t, ok)

		binaryExpr, ok := selectStmt.Where.Condition.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", binaryExpr.Operator)

		rhsLit, ok := binaryExpr.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "42", rhsLit.Literal.ToString())
	})

	t.Run("不正な WHERE 句でエラーになる", func(t *testing.T) {
		t.Run("WHERE 句の式が不完全な場合 (演算子のみ)", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * FROM users WHERE id =;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "expression")
		})

		t.Run("WHERE 句の式が不完全な場合 (左辺のみ)", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * FROM users WHERE id;"
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
			sql := "SELECT * FROM users WHERE;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "empty expression")
		})

		t.Run("AND/OR の後に式がない場合", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * FROM users WHERE id = '1' AND;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "expression")
		})
	})
}
