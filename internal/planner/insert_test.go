package planner

import (
	"minesql/internal/executor"
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/literal"
	"minesql/internal/planner/ast/statement"
	"minesql/internal/storage"
	"minesql/internal/storage/access/catalog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewInsert(t *testing.T) {
	t.Run("正常に InsertPlanner が生成される", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewInsertStmt(
			*identifier.NewTableId("users"),
			[]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("name"),
			},
			[][]literal.Literal{
				{
					literal.NewStringLiteral("'1'", "1"),
					literal.NewStringLiteral("'Alice'", "Alice"),
				},
			},
		)

		// WHEN
		planner := NewInsertPlanner(stmt)

		// THEN
		assert.NotNil(t, planner)
		assert.Equal(t, stmt, planner.Stmt)
	})

	t.Run("カラム名が空の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewInsertStmt(
			*identifier.NewTableId("users"),
			[]identifier.ColumnId{},
			[][]literal.Literal{
				{
					literal.NewStringLiteral("'1'", "1"),
					literal.NewStringLiteral("'Alice'", "Alice"),
				},
			},
		)
		planner := NewInsertPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "column names cannot be empty")
	})

	t.Run("値の数がカラム数と一致しない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewInsertStmt(
			*identifier.NewTableId("users"),
			[]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("name"),
			},
			[][]literal.Literal{
				{
					literal.NewStringLiteral("'1'", "1"),
				},
			},
		)
		planner := NewInsertPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "number of values does not match number of columns")
	})

	t.Run("挿入するレコードが空の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewInsertStmt(
			*identifier.NewTableId("users"),
			[]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("name"),
			},
			[][]literal.Literal{},
		)
		planner := NewInsertPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "records cannot be empty")
	})

	t.Run("単一レコードの挿入で Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer storage.ResetStorageManager()

		createTableForTest(t, "users", 1, nil, []*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
		})

		stmt := statement.NewInsertStmt(
			*identifier.NewTableId("users"),
			[]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("name"),
			},
			[][]literal.Literal{
				{
					literal.NewStringLiteral("'1'", "1"),
					literal.NewStringLiteral("'Alice'", "Alice"),
				},
			},
		)
		planner := NewInsertPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Insert{}, exec)
	})

	t.Run("複数レコードの挿入で Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer storage.ResetStorageManager()

		createTableForTest(t, "users", 1, nil, []*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
		})

		stmt := statement.NewInsertStmt(
			*identifier.NewTableId("users"),
			[]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("name"),
			},
			[][]literal.Literal{
				{
					literal.NewStringLiteral("'1'", "1"),
					literal.NewStringLiteral("'Alice'", "Alice"),
				},
				{
					literal.NewStringLiteral("'2'", "2"),
					literal.NewStringLiteral("'Bob'", "Bob"),
				},
				{
					literal.NewStringLiteral("'3'", "3"),
					literal.NewStringLiteral("'Charlie'", "Charlie"),
				},
			},
		)
		planner := NewInsertPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Insert{}, exec)
	})

	t.Run("複数カラムの挿入で Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer storage.ResetStorageManager()

		createTableForTest(t, "users", 1, nil, []*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
			{Name: "email", Type: catalog.ColumnTypeString},
			{Name: "age", Type: catalog.ColumnTypeString},
		})

		stmt := statement.NewInsertStmt(
			*identifier.NewTableId("users"),
			[]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("name"),
				*identifier.NewColumnId("email"),
				*identifier.NewColumnId("age"),
			},
			[][]literal.Literal{
				{
					literal.NewStringLiteral("'1'", "1"),
					literal.NewStringLiteral("'Alice'", "Alice"),
					literal.NewStringLiteral("'alice@example.com'", "alice@example.com"),
					literal.NewStringLiteral("'25'", "25"),
				},
			},
		)
		planner := NewInsertPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Insert{}, exec)
	})

	t.Run("カラム順序が異なる挿入で Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer storage.ResetStorageManager()

		createTableForTest(t, "users", 1, nil, []*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
			{Name: "email", Type: catalog.ColumnTypeString},
		})

		stmt := statement.NewInsertStmt(
			*identifier.NewTableId("users"),
			[]identifier.ColumnId{
				*identifier.NewColumnId("name"),
				*identifier.NewColumnId("email"),
				*identifier.NewColumnId("id"),
			},
			[][]literal.Literal{
				{
					literal.NewStringLiteral("'Alice'", "Alice"),
					literal.NewStringLiteral("'alice@example.com'", "alice@example.com"),
					literal.NewStringLiteral("'1'", "1"),
				},
			},
		)
		planner := NewInsertPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Insert{}, exec)
	})

	t.Run("サポートされていない literal タイプの場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer storage.ResetStorageManager()

		createTableForTest(t, "users", 1, nil, []*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
		})

		// StringLiteral 以外の literal を作成するために、カスタム literal を使用
		type UnsupportedLiteral struct {
			literal.Literal
		}
		unsupported := &UnsupportedLiteral{}

		stmt := statement.NewInsertStmt(
			*identifier.NewTableId("users"),
			[]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("name"),
			},
			[][]literal.Literal{
				{
					literal.NewStringLiteral("'1'", "1"),
					unsupported,
				},
			},
		)
		planner := NewInsertPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

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
	storage.ResetStorageManager()
	storage.InitStorageManager()
}

// テーブルを作成する
func createTableForTest(t *testing.T, tableName string, primaryKeyCount uint8, indexes []*executor.IndexParam, columns []*executor.ColumnParam) {
	createTable := executor.NewCreateTable(tableName, primaryKeyCount, indexes, columns)
	_, err := createTable.Next()
	assert.NoError(t, err)
}
