package planner

import (
	"minesql/internal/ast"
	"minesql/internal/executor"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewCreateTable(t *testing.T) {
	t.Run("正常に CreateTable が生成される", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			StmtType:  ast.StmtTypeCreate,
			Keyword:   ast.KeywordTable,
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
			},
		}

		// WHEN
		node := NewCreateTable(stmt)

		// THEN
		assert.NotNil(t, node)
		assert.Equal(t, stmt, node.Stmt)
	})
}

func TestCreateTable_Build(t *testing.T) {
	t.Run("ユニークキーなしのテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			StmtType:  ast.StmtTypeCreate,
			Keyword:   ast.KeywordTable,
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "name", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
			},
		}
		node := NewCreateTable(stmt)

		// WHEN
		exec, err := node.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.CreateTable{}, exec)
	})

	t.Run("ユニークキーインデックスがあるテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		ukDef := &ast.ConstraintUniqueKeyDef{DefType: ast.DefTypeConstraintUniqueKey, Column: *ast.NewColumnId("email")}
		ukDef.KeyName = "uk_email"

		stmt := &ast.CreateTableStmt{
			StmtType:  ast.StmtTypeCreate,
			Keyword:   ast.KeywordTable,
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "email", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
				ukDef,
			},
		}
		node := NewCreateTable(stmt)

		// WHEN
		exec, err := node.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.CreateTable{}, exec)
	})

	t.Run("プライマリキー複数、ユニークインデックス複数のテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		ukDef1 := &ast.ConstraintUniqueKeyDef{DefType: ast.DefTypeConstraintUniqueKey, Column: *ast.NewColumnId("email")}
		ukDef1.KeyName = "uk_email"

		ukDef2 := &ast.ConstraintUniqueKeyDef{DefType: ast.DefTypeConstraintUniqueKey, Column: *ast.NewColumnId("username")}
		ukDef2.KeyName = "uk_username"

		stmt := &ast.CreateTableStmt{
			StmtType:  ast.StmtTypeCreate,
			Keyword:   ast.KeywordTable,
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "tenant_id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "email", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "username", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
					*ast.NewColumnId("tenant_id"),
				}},
				ukDef1,
				ukDef2,
			},
		}
		node := NewCreateTable(stmt)

		// WHEN
		exec, err := node.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.CreateTable{}, exec)
	})

	t.Run("重複したカラム名がある場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			StmtType:  ast.StmtTypeCreate,
			Keyword:   ast.KeywordTable,
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
			},
		}
		node := NewCreateTable(stmt)

		// WHEN
		exec, err := node.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "duplicate column name")
	})

	t.Run("プライマリキーが定義されていない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			StmtType:  ast.StmtTypeCreate,
			Keyword:   ast.KeywordTable,
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "name", DataType: ast.DataTypeVarchar},
			},
		}
		node := NewCreateTable(stmt)

		// WHEN
		exec, err := node.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "primary key is required")
	})

	t.Run("複数のプライマリキーが定義されている場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			StmtType:  ast.StmtTypeCreate,
			Keyword:   ast.KeywordTable,
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "name", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
				&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{
					*ast.NewColumnId("name"),
				}},
			},
		}
		node := NewCreateTable(stmt)

		// WHEN
		exec, err := node.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "multiple primary keys defined")
	})

	t.Run("プライマリキーにカラムが指定されていない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			StmtType:  ast.StmtTypeCreate,
			Keyword:   ast.KeywordTable,
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{}},
			},
		}
		node := NewCreateTable(stmt)

		// WHEN
		exec, err := node.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "primary key must have at least one column")
	})

	t.Run("プライマリキーに指定されたカラムが存在しない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			StmtType:  ast.StmtTypeCreate,
			Keyword:   ast.KeywordTable,
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{
					*ast.NewColumnId("non_existent_column"),
				}},
			},
		}
		node := NewCreateTable(stmt)

		// WHEN
		exec, err := node.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("プライマリキーに指定されたカラムが先頭から順番でない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			StmtType:  ast.StmtTypeCreate,
			Keyword:   ast.KeywordTable,
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "name", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "email", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
					*ast.NewColumnId("email"), // name をスキップしている
				}},
			},
		}
		node := NewCreateTable(stmt)

		// WHEN
		exec, err := node.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "primary key columns must be defined in order")
	})

	t.Run("プライマリキーに指定されたカラム数が全カラム数を超える場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			StmtType:  ast.StmtTypeCreate,
			Keyword:   ast.KeywordTable,
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
					*ast.NewColumnId("name"),
					*ast.NewColumnId("email"),
				}},
			},
		}
		node := NewCreateTable(stmt)

		// WHEN
		exec, err := node.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "primary key columns exceed total columns")
	})

	t.Run("ユニークインデックスに指定されたカラムが存在しない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		ukDef := &ast.ConstraintUniqueKeyDef{DefType: ast.DefTypeConstraintUniqueKey, Column: *ast.NewColumnId("non_existent_column")}
		ukDef.KeyName = "uk_test"

		stmt := &ast.CreateTableStmt{
			StmtType:  ast.StmtTypeCreate,
			Keyword:   ast.KeywordTable,
			TableName: "users",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
				ukDef,
			},
		}
		node := NewCreateTable(stmt)

		// WHEN
		exec, err := node.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "does not exist")
	})
}
