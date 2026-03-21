package planner

import (
	"minesql/internal/ast"
	"minesql/internal/engine"
	"minesql/internal/executor"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDelete_Build(t *testing.T) {
	t.Run("存在しないテーブル名の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		stmt := &ast.DeleteStmt{
			StmtType: ast.StmtTypeDelete,
			From:     *ast.NewTableId("nonexistent"),
		}
		iterator := executor.NewTableScan(
			nil,
			nil,
			func(record executor.Record) bool { return true },
		)
		planner := NewDelete(stmt, iterator)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "table nonexistent not found")
	})

	t.Run("存在するテーブル名の場合、Delete Executor が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		tbl := getPlannerTableAccessMethod(t, "users")

		stmt := &ast.DeleteStmt{
			StmtType: ast.StmtTypeDelete,
			From:     *ast.NewTableId("users"),
		}
		iterator := executor.NewTableScan(
			tbl,
			nil,
			func(record executor.Record) bool { return true },
		)
		planner := NewDelete(stmt, iterator)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Delete{}, exec)
	})
}
