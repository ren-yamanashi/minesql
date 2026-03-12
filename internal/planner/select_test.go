package planner

import (
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/statement"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSelect(t *testing.T) {
	t.Run("正常に SelectPlanner が生成される", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewSelectStmt(
			*identifier.NewTableId("users"),
			nil,
		)

		// WHEN
		planner := NewSelectPlanner(stmt, nil)

		// THEN
		assert.NotNil(t, planner)
		assert.Equal(t, stmt, planner.Stmt)
	})
}
