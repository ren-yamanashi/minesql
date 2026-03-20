package planner

import (
	"minesql/internal/ast"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSelect(t *testing.T) {
	t.Run("正常に Select が生成される", func(t *testing.T) {
		// GIVEN
		stmt := &ast.SelectStmt{StmtType: ast.StmtTypeSelect, From: *ast.NewTableId("users"), Where: &ast.WhereClause{IsSet: false}}

		// WHEN
		planner := NewSelect(stmt, nil)

		// THEN
		assert.NotNil(t, planner)
		assert.Equal(t, stmt, planner.Stmt)
	})
}
