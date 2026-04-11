package planner

import (
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPlanSelect(t *testing.T) {
	t.Run("指定したテーブルが存在しない場合にエラーになる", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		stmt := &ast.SelectStmt{From: *ast.NewTableId("non_existent_table"), Where: nil}

		// WHEN
		exec, err := PlanSelect(0, stmt)

		// THEN
		assert.Nil(t, exec)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non_existent_table")
	})

	t.Run("Build で Project Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
		})

		stmt := &ast.SelectStmt{
			From:  *ast.NewTableId("users"),
			Where: nil,
		}

		// WHEN
		exec, err := PlanSelect(0, stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Project{}, exec)
	})

	t.Run("Project が全カラムの位置を保持する", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
			{Name: "email", Type: handler.ColumnTypeString},
		})

		stmt := &ast.SelectStmt{
			From:  *ast.NewTableId("users"),
			Where: nil,
		}

		// WHEN
		exec, err := PlanSelect(0, stmt)

		// THEN
		assert.NoError(t, err)
		proj, ok := exec.(*executor.Project)
		assert.True(t, ok)
		assert.Equal(t, []uint16{0, 1, 2}, proj.ColPos)
	})

	t.Run("WHERE句に条件が指定されている場合、Project Executorが生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
		})

		stmt := &ast.SelectStmt{
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
		exec, err := PlanSelect(0, stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Project{}, exec)
	})

	t.Run("WHERE句に存在しないカラムが指定された場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
		})

		stmt := &ast.SelectStmt{
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
		exec, err := PlanSelect(0, stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "non_existent")
	})

	t.Run("Project 経由でレコードを取得できる", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
		})

		// データを挿入
		executePlan(t, &ast.InsertStmt{
			Table: *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("name"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("1"),
					ast.NewStringLiteral("Alice"),
				},
				{
					ast.NewStringLiteral("2"),
					ast.NewStringLiteral("Bob"),
				},
			},
		})

		stmt := &ast.SelectStmt{
			From:  *ast.NewTableId("users"),
			Where: nil,
		}

		// WHEN
		hdl := handler.Get()
		trxId := hdl.BeginTrx()
		exec, err := PlanSelect(trxId, stmt)
		assert.NoError(t, err)
		results := fetchAll(t, exec)
		assert.NoError(t, hdl.CommitTrx(trxId))

		// THEN
		assert.Equal(t, 2, len(results))
		assert.Equal(t, executor.Record{[]byte("1"), []byte("Alice")}, results[0])
		assert.Equal(t, executor.Record{[]byte("2"), []byte("Bob")}, results[1])
	})
}
