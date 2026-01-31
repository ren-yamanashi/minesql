package planner

import (
	"minesql/internal/executor"
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/literal"
	"minesql/internal/planner/ast/statement"
	"minesql/internal/storage"
	"minesql/internal/storage/access/catalog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSelect(t *testing.T) {
	t.Run("正常に SelectPlanner が生成される", func(t *testing.T) {
		// GIVEN
		stmt := statement.NewSelectStmt(
			*identifier.NewTableId("users"),
			nil,
		)

		// WHEN
		planner := NewSelectPlanner(stmt)

		// THEN
		assert.NotNil(t, planner)
		assert.Equal(t, stmt, planner.Stmt)
	})

	t.Run("テーブル名が空の場合、エラーを返す", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN
		stmt := statement.NewSelectStmt(
			*identifier.NewTableId(""),
			nil,
		)
		planner := NewSelectPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "table name cannot be empty")
	})

	t.Run("WHERE 句なしで複数カラムを指定した場合、SequentialScan が生成される", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN
		stmt := statement.NewSelectStmt(
			*identifier.NewTableId("users"),
			nil,
		)
		planner := NewSelectPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.SearchTable{}, exec)
	})

	t.Run("WHERE 句でインデックス付きカラムを指定した場合、IndexScan が生成される", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN
		stmt := statement.NewSelectStmt(
			*identifier.NewTableId("users"),
			statement.NewWhereClause(
				expression.NewBinaryExpr(
					"=",
					*identifier.NewColumnId("last_name"),
					expression.NewRhsLiteral(literal.NewStringLiteral("'Doe'", "Doe")),
				),
			),
		)
		planner := NewSelectPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.SearchIndex{}, exec)
	})

	t.Run("WHERE 句でインデックスなしカラムを指定した場合、SequentialScan が生成される", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN
		stmt := statement.NewSelectStmt(
			*identifier.NewTableId("users"),
			statement.NewWhereClause(
				expression.NewBinaryExpr(
					"=",
					*identifier.NewColumnId("first_name"),
					expression.NewRhsLiteral(literal.NewStringLiteral("'John'", "John")),
				),
			),
		)
		planner := NewSelectPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.SearchTable{}, exec)
	})

	t.Run("WHERE 句で存在しないカラムを指定した場合、エラーを返す", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN
		stmt := statement.NewSelectStmt(
			*identifier.NewTableId("users"),
			statement.NewWhereClause(
				expression.NewBinaryExpr(
					"=",
					*identifier.NewColumnId("non_existent_column"),
					expression.NewRhsLiteral(literal.NewStringLiteral("'value'", "value")),
				),
			),
		)
		planner := NewSelectPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "does not exist in table")
	})

	t.Run("WHERE 句でサポートされていない型を指定した場合、エラーを返す", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN
		// サポートされていない Expression を作成
		type UnsupportedExpr struct {
			expression.Expression
		}
		unsupported := &UnsupportedExpr{}

		stmt := statement.NewSelectStmt(
			*identifier.NewTableId("users"),
			&statement.WhereClause{
				Condition: unsupported,
				IsSet:     true,
			},
		)
		planner := NewSelectPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "unsupported WHERE condition type")
	})
}

// テスト用の storage manager を初期化
func initStorageManager(t *testing.T, dataDir string) *storage.StorageManager {
	t.Setenv("MINESQL_DATA_DIR", dataDir)
	t.Setenv("MINESQL_BUFFER_SIZE", "10")

	storage.ResetStorageManager()
	storage.InitStorageManager()
	sm := storage.GetStorageManager()

	// テーブルを作成
	createTable := executor.NewCreateTable("users", 1, []*executor.IndexParam{
		{Name: "last_name", ColName: "last_name", SecondaryKey: 2},
	}, []*executor.ColumnParam{
		{Name: "id", Type: catalog.ColumnTypeString},
		{Name: "first_name", Type: catalog.ColumnTypeString},
		{Name: "last_name", Type: catalog.ColumnTypeString},
	})
	_, err := createTable.Next()
	assert.NoError(t, err)

	return sm
}
