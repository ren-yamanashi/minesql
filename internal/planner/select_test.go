package planner

import (
	"minesql/internal/ast"
	"minesql/internal/engine"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelect(t *testing.T) {
	t.Run("正常に Select が生成される", func(t *testing.T) {
		// GIVEN
		stmt := &ast.SelectStmt{StmtType: ast.StmtTypeSelect, From: *ast.NewTableId("users"), Where: &ast.WhereClause{IsSet: false}}

		// WHEN
		planner := NewSelect(stmt, nil)

		// THEN
		assert.NotNil(t, planner)
		assert.Equal(t, stmt, planner.Stmt)
	})

	t.Run("指定したテーブルが存在しない場合にエラーになる", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer engine.Reset()

		stmt := &ast.SelectStmt{StmtType: ast.StmtTypeSelect, From: *ast.NewTableId("non_existent_table"), Where: &ast.WhereClause{IsSet: false}}
		planner := NewSelect(stmt, nil)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.Nil(t, exec)
		assert.Error(t, err)
	})
}
