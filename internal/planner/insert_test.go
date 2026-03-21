package planner

import (
	"minesql/internal/ast"
	"minesql/internal/catalog"
	"minesql/internal/engine"
	"minesql/internal/executor"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewInsert(t *testing.T) {
	t.Run("正常に Insert が生成される", func(t *testing.T) {
		// GIVEN
		stmt := &ast.InsertStmt{
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
			},
		}

		// WHEN
		planner := NewInsert(stmt)

		// THEN
		assert.NotNil(t, planner)
		assert.Equal(t, stmt, planner.Stmt)
	})

	t.Run("カラム名が空の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.InsertStmt{
			StmtType: ast.StmtTypeInsert,
			Table:    *ast.NewTableId("users"),
			Cols:     []ast.ColumnId{},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("'1'", "1"),
					ast.NewStringLiteral("'Alice'", "Alice"),
				},
			},
		}
		planner := NewInsert(stmt)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "column names cannot be empty")
	})

	t.Run("値の数がカラム数と一致しない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.InsertStmt{
			StmtType: ast.StmtTypeInsert,
			Table:    *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("name"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("'1'", "1"),
				},
			},
		}
		planner := NewInsert(stmt)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "number of values does not match number of columns")
	})

	t.Run("挿入するレコードが空の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.InsertStmt{
			StmtType: ast.StmtTypeInsert,
			Table:    *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("name"),
			},
			Values: [][]ast.Literal{},
		}
		planner := NewInsert(stmt)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "records cannot be empty")
	})

	t.Run("単一レコードの挿入で Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer engine.Reset()

		createTableForTest(t, []*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
		})

		stmt := &ast.InsertStmt{
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
			},
		}
		planner := NewInsert(stmt)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Insert{}, exec)
	})

	t.Run("複数レコードの挿入で Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer engine.Reset()

		createTableForTest(t, []*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
		})

		stmt := &ast.InsertStmt{
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
				{
					ast.NewStringLiteral("'3'", "3"),
					ast.NewStringLiteral("'Charlie'", "Charlie"),
				},
			},
		}
		planner := NewInsert(stmt)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Insert{}, exec)
	})

	t.Run("複数カラムの挿入で Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer engine.Reset()

		createTableForTest(t, []*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
			{Name: "email", Type: catalog.ColumnTypeString},
			{Name: "age", Type: catalog.ColumnTypeString},
		})

		stmt := &ast.InsertStmt{
			StmtType: ast.StmtTypeInsert,
			Table:    *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("name"),
				*ast.NewColumnId("email"),
				*ast.NewColumnId("age"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("'1'", "1"),
					ast.NewStringLiteral("'Alice'", "Alice"),
					ast.NewStringLiteral("'alice@example.com'", "alice@example.com"),
					ast.NewStringLiteral("'25'", "25"),
				},
			},
		}
		planner := NewInsert(stmt)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Insert{}, exec)
	})

	t.Run("カラム順序が異なる挿入で Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer engine.Reset()

		createTableForTest(t, []*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
			{Name: "email", Type: catalog.ColumnTypeString},
		})

		stmt := &ast.InsertStmt{
			StmtType: ast.StmtTypeInsert,
			Table:    *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("name"),
				*ast.NewColumnId("email"),
				*ast.NewColumnId("id"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("'Alice'", "Alice"),
					ast.NewStringLiteral("'alice@example.com'", "alice@example.com"),
					ast.NewStringLiteral("'1'", "1"),
				},
			},
		}
		planner := NewInsert(stmt)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Insert{}, exec)
	})

	t.Run("サポートされていない literal タイプの場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer engine.Reset()

		createTableForTest(t, []*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
		})

		// StringLiteral 以外の literal を作成するために、カスタム literal を使用
		type UnsupportedLiteral struct {
			ast.Literal
		}
		unsupported := &UnsupportedLiteral{}

		stmt := &ast.InsertStmt{
			StmtType: ast.StmtTypeInsert,
			Table:    *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("name"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("'1'", "1"),
					unsupported,
				},
			},
		}
		planner := NewInsert(stmt)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "unsupported literal type")
	})
}

// StorageManager を初期化する
func initStorageManagerForTest(t *testing.T) {
	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "10")
	engine.Reset()
	engine.Init()
}

// テーブルを作成する
func createTableForTest(t *testing.T, columns []*executor.ColumnParam) {
	createTable := executor.NewCreateTable("users", 1, nil, columns)
	_, err := createTable.Next()
	assert.NoError(t, err)
}
