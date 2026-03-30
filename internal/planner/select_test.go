package planner

import (
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/engine"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelect(t *testing.T) {
	t.Run("正常に Select が生成される", func(t *testing.T) {
		// GIVEN
		stmt := &ast.SelectStmt{StmtType: ast.StmtTypeSelect, From: *ast.NewTableId("users"), Where: &ast.WhereClause{IsSet: false}}

		// WHEN
		planner := NewSelect(stmt)

		// THEN
		assert.NotNil(t, planner)
		assert.Equal(t, stmt, planner.Stmt)
	})

	t.Run("指定したテーブルが存在しない場合にエラーになる", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer engine.Reset()

		stmt := &ast.SelectStmt{StmtType: ast.StmtTypeSelect, From: *ast.NewTableId("non_existent_table"), Where: &ast.WhereClause{IsSet: false}}
		planner := NewSelect(stmt)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.Nil(t, exec)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non_existent_table")
	})

	t.Run("Build で Project Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer engine.Reset()

		createTableForTest(t, []engine.ColumnParam{
			{Name: "id", Type: engine.ColumnTypeString},
			{Name: "name", Type: engine.ColumnTypeString},
		})

		stmt := &ast.SelectStmt{
			StmtType: ast.StmtTypeSelect,
			From:     *ast.NewTableId("users"),
			Where:    &ast.WhereClause{IsSet: false},
		}
		planner := NewSelect(stmt)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Project{}, exec)
	})

	t.Run("Project が全カラムの位置を保持する", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer engine.Reset()

		createTableForTest(t, []engine.ColumnParam{
			{Name: "id", Type: engine.ColumnTypeString},
			{Name: "name", Type: engine.ColumnTypeString},
			{Name: "email", Type: engine.ColumnTypeString},
		})

		stmt := &ast.SelectStmt{
			StmtType: ast.StmtTypeSelect,
			From:     *ast.NewTableId("users"),
			Where:    &ast.WhereClause{IsSet: false},
		}
		planner := NewSelect(stmt)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.NoError(t, err)
		proj, ok := exec.(*executor.Project)
		assert.True(t, ok)
		assert.Equal(t, []uint16{0, 1, 2}, proj.ColPos)
	})

	t.Run("Project 経由でレコードを取得できる", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer engine.Reset()

		createTableForTest(t, []engine.ColumnParam{
			{Name: "id", Type: engine.ColumnTypeString},
			{Name: "name", Type: engine.ColumnTypeString},
		})

		// データを挿入
		executePlan(t, &ast.InsertStmt{
			StmtType: ast.StmtTypeInsert,
			Table:    *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("name"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("'1'", "1"),
					ast.NewStringLiteral("'Alice'", "Alice"),
				},
				{
					ast.NewStringLiteral("'2'", "2"),
					ast.NewStringLiteral("'Bob'", "Bob"),
				},
			},
		})

		stmt := &ast.SelectStmt{
			StmtType: ast.StmtTypeSelect,
			From:     *ast.NewTableId("users"),
			Where:    &ast.WhereClause{IsSet: false},
		}
		planner := NewSelect(stmt)

		// WHEN
		exec, err := planner.Build()
		assert.NoError(t, err)
		results := fetchAll(t, exec)

		// THEN
		assert.Equal(t, 2, len(results))
		assert.Equal(t, executor.Record{[]byte("1"), []byte("Alice")}, results[0])
		assert.Equal(t, executor.Record{[]byte("2"), []byte("Bob")}, results[1])
	})
}
