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
					expression.NewLhsColumn(*identifier.NewColumnId("last_name")),
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
					expression.NewLhsColumn(*identifier.NewColumnId("first_name")),
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
					expression.NewLhsColumn(*identifier.NewColumnId("non_existent_column")),
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

	t.Run("複雑な WHERE 句 (複数の AND 条件) を処理できる", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN
		// id = '1' AND first_name = 'john' AND last_name = 'doe' のような構造
		// 構造: (id = '1') AND ((first_name = 'john') AND (last_name = 'doe'))
		stmt := statement.NewSelectStmt(
			*identifier.NewTableId("users"),
			statement.NewWhereClause(
				expression.NewBinaryExpr(
					"AND",
					expression.NewLhsExpr(
						expression.NewBinaryExpr(
							"=",
							expression.NewLhsColumn(*identifier.NewColumnId("id")),
							expression.NewRhsLiteral(literal.NewStringLiteral("'1'", "1")),
						),
					),
					expression.NewRhsExpr(
						expression.NewBinaryExpr(
							"AND",
							expression.NewLhsExpr(
								expression.NewBinaryExpr(
									"=",
									expression.NewLhsColumn(*identifier.NewColumnId("first_name")),
									expression.NewRhsLiteral(literal.NewStringLiteral("'john'", "john")),
								),
							),
							expression.NewRhsExpr(
								expression.NewBinaryExpr(
									"=",
									expression.NewLhsColumn(*identifier.NewColumnId("last_name")),
									expression.NewRhsLiteral(literal.NewStringLiteral("'doe'", "doe")),
								),
							),
						),
					),
				),
			),
		)
		planner := NewSelectPlanner(stmt)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		// 複数条件の場合は Filter が使われる
		assert.IsType(t, &executor.Filter{}, exec)
	})
}

func TestComplexWhereWithData(t *testing.T) {
	t.Run("複数の AND 条件でデータをフィルタリングできる", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN: テストデータを挿入
		insertStmt := statement.NewInsertStmt(
			*identifier.NewTableId("users"),
			[]identifier.ColumnId{
				*identifier.NewColumnId("id"),
				*identifier.NewColumnId("first_name"),
				*identifier.NewColumnId("last_name"),
			},
			[][]literal.Literal{
				{
					literal.NewStringLiteral("'1'", "1"),
					literal.NewStringLiteral("'John'", "John"),
					literal.NewStringLiteral("'Doe'", "Doe"),
				},
				{
					literal.NewStringLiteral("'2'", "2"),
					literal.NewStringLiteral("'Jane'", "Jane"),
					literal.NewStringLiteral("'Smith'", "Smith"),
				},
				{
					literal.NewStringLiteral("'3'", "3"),
					literal.NewStringLiteral("'John'", "John"),
					literal.NewStringLiteral("'Johnson'", "Johnson"),
				},
			},
		)
		insertPlanner := NewInsertPlanner(insertStmt)
		insertExec, err := insertPlanner.Next()
		assert.NoError(t, err)
		_, err = insertExec.Next()
		assert.NoError(t, err)

		// WHEN: first_name = 'John' AND last_name = 'Johnson' で検索
		// 構造: (first_name = 'John') AND (last_name = 'Johnson')
		selectStmt := statement.NewSelectStmt(
			*identifier.NewTableId("users"),
			statement.NewWhereClause(
				expression.NewBinaryExpr(
					"AND",
					expression.NewLhsExpr(
						expression.NewBinaryExpr(
							"=",
							expression.NewLhsColumn(*identifier.NewColumnId("first_name")),
							expression.NewRhsLiteral(literal.NewStringLiteral("'John'", "John")),
						),
					),
					expression.NewRhsExpr(
						expression.NewBinaryExpr(
							"=",
							expression.NewLhsColumn(*identifier.NewColumnId("last_name")),
							expression.NewRhsLiteral(literal.NewStringLiteral("'Johnson'", "Johnson")),
						),
					),
				),
			),
		)
		selectPlanner := NewSelectPlanner(selectStmt)
		selectExec, err := selectPlanner.Next()
		assert.NoError(t, err)

		// THEN: id=3 のレコードのみが返される
		results := []executor.Record{}
		for {
			record, err := selectExec.Next()
			if err != nil {
				break
			}
			if len(record) == 0 {
				break
			}
			results = append(results, record)
		}

		assert.Equal(t, 1, len(results))
		assert.Equal(t, "3", string(results[0][0]))       // id
		assert.Equal(t, "John", string(results[0][1]))    // first_name
		assert.Equal(t, "Johnson", string(results[0][2])) // last_name
	})
}

func TestOperatorToCondition(t *testing.T) {
	t.Run("= 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		planner := &SelectPlanner{}
		record := executor.Record{[]byte("apple"), []byte("banana"), []byte("cherry")}

		// WHEN
		cond, err := planner.operatorToCondition("=", 1, "banana")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cond)
		assert.True(t, cond(record))
		assert.False(t, cond(executor.Record{[]byte("apple"), []byte("orange"), []byte("cherry")}))
	})

	t.Run("!= 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		planner := &SelectPlanner{}
		record := executor.Record{[]byte("apple"), []byte("banana"), []byte("cherry")}

		// WHEN
		cond, err := planner.operatorToCondition("!=", 1, "banana")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cond)
		assert.False(t, cond(record))
		assert.True(t, cond(executor.Record{[]byte("apple"), []byte("orange"), []byte("cherry")}))
	})

	t.Run("< 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		planner := &SelectPlanner{}

		// WHEN
		cond, err := planner.operatorToCondition("<", 0, "c")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cond)
		assert.True(t, cond(executor.Record{[]byte("a"), []byte("banana")}))
		assert.True(t, cond(executor.Record{[]byte("b"), []byte("banana")}))
		assert.False(t, cond(executor.Record{[]byte("c"), []byte("banana")}))
		assert.False(t, cond(executor.Record{[]byte("d"), []byte("banana")}))
	})

	t.Run("<= 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		planner := &SelectPlanner{}

		// WHEN
		cond, err := planner.operatorToCondition("<=", 0, "c")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cond)
		assert.True(t, cond(executor.Record{[]byte("a"), []byte("banana")}))
		assert.True(t, cond(executor.Record{[]byte("b"), []byte("banana")}))
		assert.True(t, cond(executor.Record{[]byte("c"), []byte("banana")}))
		assert.False(t, cond(executor.Record{[]byte("d"), []byte("banana")}))
	})

	t.Run("> 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		planner := &SelectPlanner{}

		// WHEN
		cond, err := planner.operatorToCondition(">", 0, "c")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cond)
		assert.False(t, cond(executor.Record{[]byte("a"), []byte("banana")}))
		assert.False(t, cond(executor.Record{[]byte("b"), []byte("banana")}))
		assert.False(t, cond(executor.Record{[]byte("c"), []byte("banana")}))
		assert.True(t, cond(executor.Record{[]byte("d"), []byte("banana")}))
	})

	t.Run(">= 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		planner := &SelectPlanner{}

		// WHEN
		cond, err := planner.operatorToCondition(">=", 0, "c")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cond)
		assert.False(t, cond(executor.Record{[]byte("a"), []byte("banana")}))
		assert.False(t, cond(executor.Record{[]byte("b"), []byte("banana")}))
		assert.True(t, cond(executor.Record{[]byte("c"), []byte("banana")}))
		assert.True(t, cond(executor.Record{[]byte("d"), []byte("banana")}))
	})

	t.Run("サポートされていない演算子の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		planner := &SelectPlanner{}

		// WHEN
		cond, err := planner.operatorToCondition("LIKE", 0, "pattern")

		// THEN
		assert.Error(t, err)
		assert.Nil(t, cond)
		assert.Contains(t, err.Error(), "unsupported operator")
		assert.Contains(t, err.Error(), "LIKE")
	})

	t.Run("異なる position で条件が正しく適用される", func(t *testing.T) {
		// GIVEN
		planner := &SelectPlanner{}
		record := executor.Record{[]byte("1"), []byte("John"), []byte("Doe")}

		// WHEN: position 0 (id)
		cond0, err0 := planner.operatorToCondition("=", 0, "1")
		// WHEN: position 1 (first_name)
		cond1, err1 := planner.operatorToCondition("=", 1, "John")
		// WHEN: position 2 (last_name)
		cond2, err2 := planner.operatorToCondition("=", 2, "Doe")

		// THEN
		assert.NoError(t, err0)
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.True(t, cond0(record))
		assert.True(t, cond1(record))
		assert.True(t, cond2(record))

		// 異なる値の場合は false
		assert.False(t, cond0(executor.Record{[]byte("2"), []byte("John"), []byte("Doe")}))
		assert.False(t, cond1(executor.Record{[]byte("1"), []byte("Jane"), []byte("Doe")}))
		assert.False(t, cond2(executor.Record{[]byte("1"), []byte("John"), []byte("Smith")}))
	})
}

// テスト用の storage manager を初期化
func initStorageManager(t *testing.T, dataDir string) {
	t.Setenv("MINESQL_DATA_DIR", dataDir)
	t.Setenv("MINESQL_BUFFER_SIZE", "10")

	storage.ResetStorageManager()
	storage.InitStorageManager()
	storage.GetStorageManager()

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

}
