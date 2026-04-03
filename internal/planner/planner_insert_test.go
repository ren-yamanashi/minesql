package planner

import (
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPlanInsert(t *testing.T) {
	t.Run("カラム名が空の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		var trxId handler.TrxId = 1
		stmt := &ast.InsertStmt{
			Table: *ast.NewTableId("users"),
			Cols:  []ast.ColumnId{},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("1"),
					ast.NewStringLiteral("Alice"),
				},
			},
		}

		// WHEN
		exec, err := PlanInsert(trxId, stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "column names cannot be empty")
	})

	t.Run("値の数がカラム数と一致しない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		var trxId handler.TrxId = 1
		stmt := &ast.InsertStmt{
			Table: *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("name"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("1"),
				},
			},
		}

		// WHEN
		exec, err := PlanInsert(trxId, stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "number of values does not match number of columns")
	})

	t.Run("カラム名が重複している場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		var trxId handler.TrxId = 1
		stmt := &ast.InsertStmt{
			Table: *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("name"),
				*ast.NewColumnId("id"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("1"),
					ast.NewStringLiteral("Alice"),
					ast.NewStringLiteral("2"),
				},
			},
		}

		// WHEN
		exec, err := PlanInsert(trxId, stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "duplicate column name: id")
	})

	t.Run("挿入するレコードが空の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		var trxId handler.TrxId = 1
		stmt := &ast.InsertStmt{
			Table: *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("name"),
			},
			Values: [][]ast.Literal{},
		}

		// WHEN
		exec, err := PlanInsert(trxId, stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "records cannot be empty")
	})

	t.Run("存在しないテーブル名の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		stmt := &ast.InsertStmt{
			Table: *ast.NewTableId("nonexistent"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("1"),
				},
			},
		}

		// WHEN
		exec, err := PlanInsert(trxId, stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "table nonexistent not found")
	})

	t.Run("単一レコードの挿入で Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
		})

		stmt := &ast.InsertStmt{
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
			},
		}

		// WHEN
		exec, err := PlanInsert(trxId, stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Insert{}, exec)
	})

	t.Run("複数レコードの挿入で Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
		})

		stmt := &ast.InsertStmt{
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
				{
					ast.NewStringLiteral("3"),
					ast.NewStringLiteral("Charlie"),
				},
			},
		}

		// WHEN
		exec, err := PlanInsert(trxId, stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Insert{}, exec)
	})

	t.Run("複数カラムの挿入で Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
			{Name: "email", Type: handler.ColumnTypeString},
			{Name: "age", Type: handler.ColumnTypeString},
		})

		stmt := &ast.InsertStmt{
			Table: *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("name"),
				*ast.NewColumnId("email"),
				*ast.NewColumnId("age"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("1"),
					ast.NewStringLiteral("Alice"),
					ast.NewStringLiteral("alice@example.com"),
					ast.NewStringLiteral("25"),
				},
			},
		}

		// WHEN
		exec, err := PlanInsert(trxId, stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Insert{}, exec)
	})

	t.Run("カラム順序が異なる挿入で Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
			{Name: "email", Type: handler.ColumnTypeString},
		})

		stmt := &ast.InsertStmt{
			Table: *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("name"),
				*ast.NewColumnId("email"),
				*ast.NewColumnId("id"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("Alice"),
					ast.NewStringLiteral("alice@example.com"),
					ast.NewStringLiteral("1"),
				},
			},
		}

		// WHEN
		exec, err := PlanInsert(trxId, stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Insert{}, exec)
	})

	t.Run("テーブルに存在しないカラムを指定した場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
		})

		stmt := &ast.InsertStmt{
			Table: *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("non_existent"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("1"),
					ast.NewStringLiteral("Alice"),
				},
			},
		}

		// WHEN
		exec, err := PlanInsert(trxId, stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "column does not exist")
	})

	t.Run("サポートされていない literal タイプの場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
		})

		// StringLiteral 以外の literal を作成するために、カスタム literal を使用
		type UnsupportedLiteral struct {
			ast.Literal
		}
		unsupported := &UnsupportedLiteral{}

		stmt := &ast.InsertStmt{
			Table: *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("name"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("1"),
					unsupported,
				},
			},
		}

		// WHEN
		exec, err := PlanInsert(trxId, stmt)

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
	handler.Reset()
	handler.Init()
}

// テーブルを作成する
func createTableForTest(t *testing.T, columns []handler.CreateColumnParam) {
	createTable := executor.NewCreateTable("users", 1, nil, columns)
	_, err := createTable.Next()
	assert.NoError(t, err)
}
