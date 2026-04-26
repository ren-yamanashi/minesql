package planner

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/ast"
	"github.com/ren-yamanashi/minesql/internal/executor"
	"github.com/ren-yamanashi/minesql/internal/storage/handler"
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
		assert.Contains(t, err.Error(), "duplicate index name: uk_same")
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
		assert.Contains(t, err.Error(), "column 'email' cannot have multiple indexes")
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

	t.Run("非ユニークインデックスがあるテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			TableName: "products",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "category", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
				&ast.ConstraintKeyDef{KeyName: "idx_category", Column: *ast.NewColumnId("category")},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
	})

	t.Run("ユニークと非ユニークのインデックスが混在するテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			TableName: "products",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "name", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "category", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
				&ast.ConstraintUniqueKeyDef{KeyName: "idx_name", Column: *ast.NewColumnId("name")},
				&ast.ConstraintKeyDef{KeyName: "idx_category", Column: *ast.NewColumnId("category")},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
	})

	t.Run("UNIQUE KEY と KEY でインデックス名が重複する場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			TableName: "products",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "name", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "category", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
				&ast.ConstraintUniqueKeyDef{KeyName: "idx_same", Column: *ast.NewColumnId("name")},
				&ast.ConstraintKeyDef{KeyName: "idx_same", Column: *ast.NewColumnId("category")},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "duplicate index name: idx_same")
	})

	t.Run("非ユニークインデックスに指定されたカラムが存在しない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := &ast.CreateTableStmt{
			TableName: "products",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
				&ast.ConstraintKeyDef{KeyName: "idx_nonexistent", Column: *ast.NewColumnId("nonexistent")},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("FK 制約付きのテーブルを作成できる", func(t *testing.T) {
		// GIVEN: 親テーブルをカタログに登録
		initStorageManagerForTest(t)
		defer handler.Reset()

		parentTable := executor.NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "string"},
		}, nil)
		_, err := parentTable.Next()
		assert.NoError(t, err)

		stmt := &ast.CreateTableStmt{
			TableName: "orders",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "user_id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
				&ast.ConstraintKeyDef{KeyName: "idx_user_id", Column: *ast.NewColumnId("user_id")},
				&ast.ConstraintForeignKeyDef{KeyName: "fk_user", Column: *ast.NewColumnId("user_id"), RefTable: "users", RefColumn: "id"},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
	})

	t.Run("FK の参照先テーブルが存在しない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		stmt := &ast.CreateTableStmt{
			TableName: "orders",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "user_id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
				&ast.ConstraintKeyDef{KeyName: "idx_user_id", Column: *ast.NewColumnId("user_id")},
				&ast.ConstraintForeignKeyDef{KeyName: "fk_user", Column: *ast.NewColumnId("user_id"), RefTable: "users", RefColumn: "id"},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "referenced table 'users' does not exist")
	})

	t.Run("FK の参照先カラムが存在しない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		parentTable := executor.NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "string"},
		}, nil)
		_, err := parentTable.Next()
		assert.NoError(t, err)

		stmt := &ast.CreateTableStmt{
			TableName: "orders",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "user_id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
				&ast.ConstraintKeyDef{KeyName: "idx_user_id", Column: *ast.NewColumnId("user_id")},
				&ast.ConstraintForeignKeyDef{KeyName: "fk_user", Column: *ast.NewColumnId("user_id"), RefTable: "users", RefColumn: "nonexistent"},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "referenced column 'nonexistent' does not exist in table 'users'")
	})

	t.Run("FK の参照先カラムに PK も UNIQUE KEY もない場合、エラーを返す", func(t *testing.T) {
		// GIVEN: 参照先カラムが非ユニークインデックスしか持たないテーブル
		initStorageManagerForTest(t)
		defer handler.Reset()

		parentTable := executor.NewCreateTable("users", 1,
			[]handler.CreateIndexParam{{Name: "idx_name", ColName: "name", ColIdx: 1, Unique: false}},
			[]handler.CreateColumnParam{
				{Name: "id", Type: "string"},
				{Name: "name", Type: "string"},
			}, nil)
		_, err := parentTable.Next()
		assert.NoError(t, err)

		stmt := &ast.CreateTableStmt{
			TableName: "orders",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "user_name", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
				&ast.ConstraintKeyDef{KeyName: "idx_user_name", Column: *ast.NewColumnId("user_name")},
				&ast.ConstraintForeignKeyDef{KeyName: "fk_user", Column: *ast.NewColumnId("user_name"), RefTable: "users", RefColumn: "name"},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "must be a primary key or have a unique index")
	})

	t.Run("自己参照 FK の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		stmt := &ast.CreateTableStmt{
			TableName: "categories",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "parent_id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
				&ast.ConstraintKeyDef{KeyName: "idx_parent_id", Column: *ast.NewColumnId("parent_id")},
				&ast.ConstraintForeignKeyDef{KeyName: "fk_parent", Column: *ast.NewColumnId("parent_id"), RefTable: "categories", RefColumn: "id"},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "self-referencing foreign key is not supported")
	})

	t.Run("FK カラムが存在しない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		parentTable := executor.NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "string"},
		}, nil)
		_, err := parentTable.Next()
		assert.NoError(t, err)

		stmt := &ast.CreateTableStmt{
			TableName: "orders",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
				&ast.ConstraintForeignKeyDef{KeyName: "fk_user", Column: *ast.NewColumnId("nonexistent"), RefTable: "users", RefColumn: "id"},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "foreign key column 'nonexistent' does not exist")
	})

	t.Run("同一 CREATE TABLE 内で FK 名が重複する場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		parentTable1 := executor.NewCreateTable("t1", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "string"},
		}, nil)
		_, err := parentTable1.Next()
		assert.NoError(t, err)
		parentTable2 := executor.NewCreateTable("t2", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "string"},
		}, nil)
		_, err = parentTable2.Next()
		assert.NoError(t, err)

		stmt := &ast.CreateTableStmt{
			TableName: "orders",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "col1", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "col2", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
				&ast.ConstraintKeyDef{KeyName: "idx_col1", Column: *ast.NewColumnId("col1")},
				&ast.ConstraintKeyDef{KeyName: "idx_col2", Column: *ast.NewColumnId("col2")},
				&ast.ConstraintForeignKeyDef{KeyName: "fk_dup", Column: *ast.NewColumnId("col1"), RefTable: "t1", RefColumn: "id"},
				&ast.ConstraintForeignKeyDef{KeyName: "fk_dup", Column: *ast.NewColumnId("col2"), RefTable: "t2", RefColumn: "id"},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "duplicate foreign key constraint name: 'fk_dup'")
	})

	t.Run("FK カラムにインデックスがない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		parentTable := executor.NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "string"},
		}, nil)
		_, err := parentTable.Next()
		assert.NoError(t, err)

		stmt := &ast.CreateTableStmt{
			TableName: "orders",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "user_id", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
				// KEY idx_user_id (user_id) がない
				&ast.ConstraintForeignKeyDef{KeyName: "fk_user", Column: *ast.NewColumnId("user_id"), RefTable: "users", RefColumn: "id"},
			},
		}

		// WHEN
		exec, err := PlanCreateTable(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "foreign key column 'user_id' must have an index")
	})
}
