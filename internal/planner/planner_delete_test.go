package planner

import (
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPlanDelete(t *testing.T) {
	t.Run("存在しないテーブル名の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		stmt := &ast.DeleteStmt{
			From: *ast.NewTableId("nonexistent"),
		}

		// WHEN
		exec, err := PlanDelete(trxId, stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "nonexistent")
	})

	t.Run("存在するテーブル名の場合、Delete Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		createTableForTest(t, nil)

		stmt := &ast.DeleteStmt{
			From:  *ast.NewTableId("users"),
			Where: nil,
		}

		// WHEN
		exec, err := PlanDelete(trxId, stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Delete{}, exec)
	})

	t.Run("WHERE句に条件が指定されている場合、Delete Executorが生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
		})

		stmt := &ast.DeleteStmt{
			From: *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("id")),
					ast.NewRhsLiteral(ast.NewStringLiteral("1")),
				),
			},
		}

		// WHEN
		exec, err := PlanDelete(trxId, stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Delete{}, exec)
	})

	t.Run("WHERE句に存在しないカラムが指定された場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
		})

		stmt := &ast.DeleteStmt{
			From: *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("non_existent")),
					ast.NewRhsLiteral(ast.NewStringLiteral("test")),
				),
			},
		}

		// WHEN
		exec, err := PlanDelete(trxId, stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "non_existent")
	})
}
