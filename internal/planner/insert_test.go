package planner

import (
	"minesql/internal/executor"
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/literal"
	"minesql/internal/planner/ast/statement"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewInsert(t *testing.T) {
	t.Run("正常に InsertPlanner が生成される", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewInsertStmt(
			*identifier.NewTableId("users", ""),
			[]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("name"),
			},
			[][]literal.Literal{
				{
					literal.NewStringLiteral("'1'", "1"),
					literal.NewStringLiteral("'Alice'", "Alice"),
				},
			},
		)

		// WHEN
		planner := NewInsertPlanner(stmt)

		// THEN
		assert.NotNil(t, planner)
		assert.Equal(t, stmt, planner.Stmt)
	})

	t.Run("単一レコードの挿入で Executor が生成される", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewInsertStmt(
			*identifier.NewTableId("users", ""),
			[]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("name"),
			},
			[][]literal.Literal{
				{
					literal.NewStringLiteral("'1'", "1"),
					literal.NewStringLiteral("'Alice'", "Alice"),
				},
			},
		)
		planner := NewInsertPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Insert{}, exec)
	})

	t.Run("複数レコードの挿入で Executor が生成される", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewInsertStmt(
			*identifier.NewTableId("users", ""),
			[]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("name"),
			},
			[][]literal.Literal{
				{
					literal.NewStringLiteral("'1'", "1"),
					literal.NewStringLiteral("'Alice'", "Alice"),
				},
				{
					literal.NewStringLiteral("'2'", "2"),
					literal.NewStringLiteral("'Bob'", "Bob"),
				},
				{
					literal.NewStringLiteral("'3'", "3"),
					literal.NewStringLiteral("'Charlie'", "Charlie"),
				},
			},
		)
		planner := NewInsertPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Insert{}, exec)
	})

	t.Run("複数カラムの挿入で Executor が生成される", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewInsertStmt(
			*identifier.NewTableId("users", ""),
			[]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("name"),
				*identifier.NewColumnId("email"),
				*identifier.NewColumnId("age"),
			},
			[][]literal.Literal{
				{
					literal.NewStringLiteral("'1'", "1"),
					literal.NewStringLiteral("'Alice'", "Alice"),
					literal.NewStringLiteral("'alice@example.com'", "alice@example.com"),
					literal.NewStringLiteral("'25'", "25"),
				},
			},
		)
		planner := NewInsertPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Insert{}, exec)
	})

	t.Run("サポートされていない literal タイプの場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		// StringLiteral 以外の literal を作成するために、カスタム literal を使用
		type UnsupportedLiteral struct {
			literal.Literal
		}
		unsupported := &UnsupportedLiteral{}

		stmt := statement.NewInsertStmt(
			*identifier.NewTableId("users", ""),
			[]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("name"),
			},
			[][]literal.Literal{
				{
					literal.NewStringLiteral("'1'", "1"),
					unsupported,
				},
			},
		)
		planner := NewInsertPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "unsupported literal type")
	})
}
