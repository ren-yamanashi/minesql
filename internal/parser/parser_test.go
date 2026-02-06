package parser

import (
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/statement"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParser_SimpleSelect(t *testing.T) {
	t.Run("WHERE 句なしの SELECT 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users"
		parser := NewParser(nil)

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*statement.SelectStmt)
		assert.True(t, ok, "expected *statement.SelectStmt, got %T", result)

		assert.Equal(t, statement.StmtTypeSelect, selectStmt.StmtType)
		assert.Equal(t, "users", selectStmt.From.TableName)
		assert.Equal(t, identifier.IdTypeTable, selectStmt.From.IdType)
		assert.Nil(t, selectStmt.Where)
	})

	t.Run("WHERE 句付きの SELECT 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id = '1'"
		parser := NewParser(nil)

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*statement.SelectStmt)
		assert.True(t, ok, "expected *statement.SelectStmt, got %T", result)

		assert.Equal(t, statement.StmtTypeSelect, selectStmt.StmtType)
		assert.Equal(t, "users", selectStmt.From.TableName)

		assert.NotNil(t, selectStmt.Where)
		assert.True(t, selectStmt.Where.IsSet)
		assert.NotNil(t, selectStmt.Where.Condition)

		binaryExpr, ok := selectStmt.Where.Condition.(*expression.BinaryExpr)
		assert.True(t, ok, "expected *expression.BinaryExpr, got %T", selectStmt.Where.Condition)
		assert.Equal(t, "=", binaryExpr.Operator)

		lhsCol, ok := binaryExpr.Left.(*expression.LhsColumn)
		assert.True(t, ok, "expected *expression.LhsColumn, got %T", binaryExpr.Left)
		assert.Equal(t, "id", lhsCol.Column.ColName)

		rhsLit, ok := binaryExpr.Right.(*expression.RhsLiteral)
		assert.True(t, ok, "expected *expression.RhsLiteral, got %T", binaryExpr.Right)
		assert.Equal(t, "1", rhsLit.Literal.ToString())
	})
}
