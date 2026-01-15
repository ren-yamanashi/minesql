package planner

import (
	"minesql/internal/executor"
	"minesql/internal/planner/ast/definition"
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/statement"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewCreateTableNode(t *testing.T) {
	t.Run("正常に CreateTableNode が生成される", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewCreateTableStmt("users", []definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeInt),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("id"),
			}),
		})

		// WHEN
		node := NewCreateTableNode(stmt)

		// THEN
		assert.NotNil(t, node)
		assert.Equal(t, stmt, node.Stmt)
	})
}

func TestCreateTableNode_Next(t *testing.T) {
	t.Run("ユニークキーなしのテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewCreateTableStmt("users", []definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeInt),
			definition.NewColumnDef("name", definition.DataTypeVarchar),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("id"),
			}),
		})
		node := NewCreateTableNode(stmt)

		// WHEN
		exec, err := node.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.CreateTable{}, exec)
	})

	t.Run("ユニークキーインデックスがあるテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		ukDef := definition.NewConstraintUniqueKeyDef([]identifier.ColumnId{
			*identifier.NewColumnId("email"),
		})
		ukDef.KeyName = "uk_email"

		stmt := statement.NewCreateTableStmt("users", []definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeInt),
			definition.NewColumnDef("email", definition.DataTypeVarchar),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("id"),
			}),
			ukDef,
		})
		node := NewCreateTableNode(stmt)

		// WHEN
		exec, err := node.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.CreateTable{}, exec)
	})

	t.Run("プライマリキー複数、ユニークインデックス複数のテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		ukDef1 := definition.NewConstraintUniqueKeyDef([]identifier.ColumnId{
			*identifier.NewColumnId("email"),
		})
		ukDef1.KeyName = "uk_email"

		ukDef2 := definition.NewConstraintUniqueKeyDef([]identifier.ColumnId{
			*identifier.NewColumnId("username"),
		})
		ukDef2.KeyName = "uk_username"

		stmt := statement.NewCreateTableStmt("users", []definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeInt),
			definition.NewColumnDef("tenant_id", definition.DataTypeInt),
			definition.NewColumnDef("email", definition.DataTypeVarchar),
			definition.NewColumnDef("username", definition.DataTypeVarchar),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("tenant_id"),
			}),
			ukDef1,
			ukDef2,
		})
		node := NewCreateTableNode(stmt)

		// WHEN
		exec, err := node.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.CreateTable{}, exec)
	})

	t.Run("重複したカラム名がある場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewCreateTableStmt("users", []definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeInt),
			definition.NewColumnDef("id", definition.DataTypeVarchar),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("id"),
			}),
		})
		node := NewCreateTableNode(stmt)

		// WHEN
		exec, err := node.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "duplicate column name")
	})

	t.Run("プライマリキーが定義されていない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewCreateTableStmt("users", []definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeInt),
			definition.NewColumnDef("name", definition.DataTypeVarchar),
		})
		node := NewCreateTableNode(stmt)

		// WHEN
		exec, err := node.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "primary key is required")
	})

	t.Run("複数のプライマリキーが定義されている場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewCreateTableStmt("users", []definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeInt),
			definition.NewColumnDef("name", definition.DataTypeVarchar),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("id"),
			}),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("name"),
			}),
		})
		node := NewCreateTableNode(stmt)

		// WHEN
		exec, err := node.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "multiple primary keys defined")
	})

	t.Run("プライマリキーにカラムが指定されていない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewCreateTableStmt("users", []definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeInt),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{}),
		})
		node := NewCreateTableNode(stmt)

		// WHEN
		exec, err := node.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "primary key must have at least one column")
	})

	t.Run("プライマリキーに指定されたカラムが存在しない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewCreateTableStmt("users", []definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeInt),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("non_existent_column"),
			}),
		})
		node := NewCreateTableNode(stmt)

		// WHEN
		exec, err := node.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("プライマリキーに指定されたカラムが先頭から順番でない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewCreateTableStmt("users", []definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeInt),
			definition.NewColumnDef("name", definition.DataTypeVarchar),
			definition.NewColumnDef("email", definition.DataTypeVarchar),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("email"), // name をスキップしている
			}),
		})
		node := NewCreateTableNode(stmt)

		// WHEN
		exec, err := node.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "primary key columns must be defined in order")
	})

	t.Run("プライマリキーに指定されたカラム数が全カラム数を超える場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewCreateTableStmt("users", []definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeInt),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("name"),
				*identifier.NewColumnId("email"),
			}),
		})
		node := NewCreateTableNode(stmt)

		// WHEN
		exec, err := node.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "primary key columns exceed total columns")
	})

	t.Run("ユニークインデックスにカラムが指定されていない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		ukDef := definition.NewConstraintUniqueKeyDef([]identifier.ColumnId{})
		ukDef.KeyName = "uk_test"

		stmt := statement.NewCreateTableStmt("users", []definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeInt),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("id"),
			}),
			ukDef,
		})
		node := NewCreateTableNode(stmt)

		// WHEN
		exec, err := node.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "must have at least one column")
	})

	t.Run("ユニークインデックスに指定されたカラムが存在しない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		ukDef := definition.NewConstraintUniqueKeyDef([]identifier.ColumnId{
			*identifier.NewColumnId("non_existent_column"),
		})
		ukDef.KeyName = "uk_test"

		stmt := statement.NewCreateTableStmt("users", []definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeInt),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("id"),
			}),
			ukDef,
		})
		node := NewCreateTableNode(stmt)

		// WHEN
		exec, err := node.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("ユニークインデックスが複数カラムで構成されるの場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		ukDef := definition.NewConstraintUniqueKeyDef([]identifier.ColumnId{
			*identifier.NewColumnId("email"),
			*identifier.NewColumnId("username"),
		})
		ukDef.KeyName = "uk_email_username"

		stmt := statement.NewCreateTableStmt("users", []definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeInt),
			definition.NewColumnDef("email", definition.DataTypeVarchar),
			definition.NewColumnDef("username", definition.DataTypeVarchar),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("id"),
			}),
			ukDef,
		})
		node := NewCreateTableNode(stmt)

		// WHEN
		exec, err := node.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "only single-column unique keys are supported")
	})
}
