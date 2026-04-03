package planner

import (
	"minesql/internal/ast"
	"minesql/internal/executor"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPlanCreateTable(t *testing.T) {
	t.Run("ユニークキーなしのテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "name", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.CreateTable{}, exec)
	})

	t.Run("ユニークキーインデックスがあるテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		ukDef := &ast.ConstraintUniqueKeyDef{Column: *ast.NewColumnId("email")}
		ukDef.KeyName = "uk_email"

		stmt := &ast.CreateTableStmt{
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "email", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
				ukDef,
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.CreateTable{}, exec)
	})

	t.Run("プライマリキー複数、ユニークインデックス複数のテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		ukDef1 := &ast.ConstraintUniqueKeyDef{Column: *ast.NewColumnId("email")}
		ukDef1.KeyName = "uk_email"

		ukDef2 := &ast.ConstraintUniqueKeyDef{Column: *ast.NewColumnId("username")}
		ukDef2.KeyName = "uk_username"

		stmt := &ast.CreateTableStmt{
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "tenant_id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "email", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "username", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
					*ast.NewColumnId("tenant_id"),
				}},
				ukDef1,
				ukDef2,
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.CreateTable{}, exec)
	})

	t.Run("重複したカラム名がある場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "duplicate column name")
	})

	t.Run("プライマリキーが定義されていない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "name", DataType: ast.DataTypeVarchar},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "primary key is required")
	})

	t.Run("複数のプライマリキーが定義されている場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "name", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("name"),
				}},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "multiple primary keys defined")
	})

	t.Run("プライマリキーにカラムが指定されていない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{}},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "primary key must have at least one column")
	})

	t.Run("プライマリキーに指定されたカラムが存在しない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("non_existent_column"),
				}},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("プライマリキーに指定されたカラムが先頭から順番でない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "name", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "email", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
					*ast.NewColumnId("email"), // name をスキップしている
				}},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "primary key columns must be defined in order")
	})

	t.Run("プライマリキーに指定されたカラム数が全カラム数を超える場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
					*ast.NewColumnId("name"),
					*ast.NewColumnId("email"),
				}},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "primary key columns exceed total columns")
	})

	t.Run("カラム定義がない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "table must have at least one column")
	})

	t.Run("重複したユニークキー名がある場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		ukDef1 := &ast.ConstraintUniqueKeyDef{Column: *ast.NewColumnId("email")}
		ukDef1.KeyName = "uk_same"

		ukDef2 := &ast.ConstraintUniqueKeyDef{Column: *ast.NewColumnId("username")}
		ukDef2.KeyName = "uk_same"

		stmt := &ast.CreateTableStmt{
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "email", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "username", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
				ukDef1,
				ukDef2,
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "duplicate unique key name: uk_same")
	})

	t.Run("同一カラムが複数のユニークキーに指定されている場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		ukDef1 := &ast.ConstraintUniqueKeyDef{Column: *ast.NewColumnId("email")}
		ukDef1.KeyName = "uk_email1"

		ukDef2 := &ast.ConstraintUniqueKeyDef{Column: *ast.NewColumnId("email")}
		ukDef2.KeyName = "uk_email2"

		stmt := &ast.CreateTableStmt{
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "email", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
				ukDef1,
				ukDef2,
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "column 'email' cannot be part of multiple unique keys")
	})

	t.Run("カラムが1つだけのテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			TableName: "counters",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.CreateTable{}, exec)
	})

	t.Run("ユニークインデックスに指定されたカラムが存在しない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		ukDef := &ast.ConstraintUniqueKeyDef{Column: *ast.NewColumnId("non_existent_column")}
		ukDef.KeyName = "uk_test"

		stmt := &ast.CreateTableStmt{
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
				ukDef,
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "does not exist")
	})
}
