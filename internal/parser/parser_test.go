package parser

import (
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/statement"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParserSelect(t *testing.T) {
	t.Run("WHERE 句なしの SELECT 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*statement.SelectStmt)
		assert.True(t, ok)

		assert.Equal(t, statement.StmtTypeSelect, selectStmt.StmtType)
		assert.Equal(t, "users", selectStmt.From.TableName)
		assert.Equal(t, identifier.IdTypeTable, selectStmt.From.IdType)
		assert.Nil(t, selectStmt.Where)
	})

	t.Run("WHERE 句付きの SELECT 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id = '1'"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*statement.SelectStmt)
		assert.True(t, ok)

		assert.Equal(t, statement.StmtTypeSelect, selectStmt.StmtType)
		assert.Equal(t, "users", selectStmt.From.TableName)

		assert.NotNil(t, selectStmt.Where)
		assert.True(t, selectStmt.Where.IsSet)
		assert.NotNil(t, selectStmt.Where.Condition)

		binaryExpr, ok := selectStmt.Where.Condition.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", binaryExpr.Operator)

		lhsCol, ok := binaryExpr.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "id", lhsCol.Column.ColName)

		rhsLit, ok := binaryExpr.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "1", rhsLit.Literal.ToString())
	})

	t.Run("WHERE 句に AND があるケースをパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id = '1' AND name = 'John'"
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

	t.Run("WHERE 句に OR があるケースをパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id = '1' OR id = '2'"
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

	t.Run("WHERE 句に AND と OR の両方があるケースをパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id = '1' AND name = 'John' OR name = 'Jane'"
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
		assert.NotNil(t, selectStmt.Where.Condition)

		// 最上位は OR
		orExpr, ok := selectStmt.Where.Condition.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "OR", orExpr.Operator)

		// OR の左辺: (id = '1' AND name = 'John')
		leftExpr, ok := orExpr.Left.(*expression.LhsExpr)
		assert.True(t, ok)
		andExpr, ok := leftExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "AND", andExpr.Operator)

		// AND の左辺: id = '1'
		andLeftExpr, ok := andExpr.Left.(*expression.LhsExpr)
		assert.True(t, ok)
		idEqExpr, ok := andLeftExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", idEqExpr.Operator)

		idCol, ok := idEqExpr.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "id", idCol.Column.ColName)

		idLit, ok := idEqExpr.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "1", idLit.Literal.ToString())

		// AND の右辺: name = 'John'
		andRightExpr, ok := andExpr.Right.(*expression.RhsExpr)
		assert.True(t, ok)
		nameJohnEqExpr, ok := andRightExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", nameJohnEqExpr.Operator)

		nameJohnCol, ok := nameJohnEqExpr.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "name", nameJohnCol.Column.ColName)

		nameJohnLit, ok := nameJohnEqExpr.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "John", nameJohnLit.Literal.ToString())

		// OR の右辺: name = 'Jane'
		orRightExpr, ok := orExpr.Right.(*expression.RhsExpr)
		assert.True(t, ok)
		nameJaneEqExpr, ok := orRightExpr.Expr.(*expression.BinaryExpr)
		assert.True(t, ok)
		assert.Equal(t, "=", nameJaneEqExpr.Operator)

		nameJaneCol, ok := nameJaneEqExpr.Left.(*expression.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "name", nameJaneCol.Column.ColName)

		nameJaneLit, ok := nameJaneEqExpr.Right.(*expression.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "Jane", nameJaneLit.Literal.ToString())
	})
}
