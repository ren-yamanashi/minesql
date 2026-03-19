package planner

import (
	"minesql/internal/executor"
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/literal"
	"minesql/internal/planner/ast/statement"
	"minesql/internal/storage"
	"minesql/internal/storage/catalog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSearchPlanner(t *testing.T) {
	t.Run("テーブル名が空の場合、エラーを返す", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN
		search := NewSearchPlanner("", nil)

		// WHEN
		exec, err := search.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "table name cannot be empty")
	})

	t.Run("WHERE 句なしの場合、SequentialScan が生成される", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN
		search := NewSearchPlanner("users", nil)

		// WHEN
		exec, err := search.Next()

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
		where := statement.NewWhereClause(
			expression.NewBinaryExpr(
				"=",
				expression.NewLhsColumn(*identifier.NewColumnId("last_name")),
				expression.NewRhsLiteral(literal.NewStringLiteral("'Doe'", "Doe")),
			),
		)
		search := NewSearchPlanner("users", where)

		// WHEN
		exec, err := search.Next()

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
		where := statement.NewWhereClause(
			expression.NewBinaryExpr(
				"=",
				expression.NewLhsColumn(*identifier.NewColumnId("first_name")),
				expression.NewRhsLiteral(literal.NewStringLiteral("'John'", "John")),
			),
		)
		search := NewSearchPlanner("users", where)

		// WHEN
		exec, err := search.Next()

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
		where := statement.NewWhereClause(
			expression.NewBinaryExpr(
				"=",
				expression.NewLhsColumn(*identifier.NewColumnId("non_existent_column")),
				expression.NewRhsLiteral(literal.NewStringLiteral("'value'", "value")),
			),
		)
		search := NewSearchPlanner("users", where)

		// WHEN
		exec, err := search.Next()

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
		type UnsupportedExpr struct {
			expression.Expression
		}
		unsupported := &UnsupportedExpr{}
		where := &statement.WhereClause{
			Condition: unsupported,
			IsSet:     true,
		}
		search := NewSearchPlanner("users", where)

		// WHEN
		exec, err := search.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "unsupported WHERE condition type")
	})

	t.Run("複数の AND 条件で Filter が生成される", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN: (id = '1') AND ((first_name = 'john') AND (last_name = 'doe'))
		where := statement.NewWhereClause(
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
		)
		search := NewSearchPlanner("users", where)

		// WHEN
		exec, err := search.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Filter{}, exec)
	})

	t.Run("AND と OR の混合条件で Filter が生成される", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN: (first_name = 'John') OR ((id = '1') AND (last_name = 'Doe'))
		where := statement.NewWhereClause(
			expression.NewBinaryExpr(
				"OR",
				expression.NewLhsExpr(
					expression.NewBinaryExpr(
						"=",
						expression.NewLhsColumn(*identifier.NewColumnId("first_name")),
						expression.NewRhsLiteral(literal.NewStringLiteral("'John'", "John")),
					),
				),
				expression.NewRhsExpr(
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
								"=",
								expression.NewLhsColumn(*identifier.NewColumnId("last_name")),
								expression.NewRhsLiteral(literal.NewStringLiteral("'Doe'", "Doe")),
							),
						),
					),
				),
			),
		)
		search := NewSearchPlanner("users", where)

		// WHEN
		exec, err := search.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Filter{}, exec)
	})

	t.Run("LHS がカラム、RHS が式の場合、エラーを返す", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN: WHERE last_name = (first_name = 'John') のような不正な構造
		where := statement.NewWhereClause(
			expression.NewBinaryExpr(
				"=",
				expression.NewLhsColumn(*identifier.NewColumnId("last_name")),
				expression.NewRhsExpr(
					expression.NewBinaryExpr(
						"=",
						expression.NewLhsColumn(*identifier.NewColumnId("first_name")),
						expression.NewRhsLiteral(literal.NewStringLiteral("'John'", "John")),
					),
				),
			),
		)
		search := NewSearchPlanner("users", where)

		// WHEN
		exec, err := search.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "when LHS is a column, RHS must be a literal")
	})

	t.Run("OR 演算子を使った場合、Filter が生成される", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN: WHERE (first_name = 'John') OR (last_name = 'Doe') のような構造
		where := statement.NewWhereClause(
			expression.NewBinaryExpr(
				"OR",
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
						expression.NewRhsLiteral(literal.NewStringLiteral("'Doe'", "Doe")),
					),
				),
			),
		)
		search := NewSearchPlanner("users", where)

		// WHEN
		exec, err := search.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Filter{}, exec)
	})

	t.Run("条件内で存在しないカラムを指定した場合、エラーを返す", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN: WHERE (non_existent = 'value') AND (last_name = 'Doe')
		where := statement.NewWhereClause(
			expression.NewBinaryExpr(
				"AND",
				expression.NewLhsExpr(
					expression.NewBinaryExpr(
						"=",
						expression.NewLhsColumn(*identifier.NewColumnId("non_existent")),
						expression.NewRhsLiteral(literal.NewStringLiteral("'value'", "value")),
					),
				),
				expression.NewRhsExpr(
					expression.NewBinaryExpr(
						"=",
						expression.NewLhsColumn(*identifier.NewColumnId("last_name")),
						expression.NewRhsLiteral(literal.NewStringLiteral("'Doe'", "Doe")),
					),
				),
			),
		)
		search := NewSearchPlanner("users", where)

		// WHEN
		exec, err := search.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "does not exist in table")
	})

	t.Run("条件内で LHS がカラム、RHS が式の場合、エラーを返す", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN: WHERE (first_name = (last_name = 'Doe')) AND (id = '1')
		where := statement.NewWhereClause(
			expression.NewBinaryExpr(
				"AND",
				expression.NewLhsExpr(
					expression.NewBinaryExpr(
						"=",
						expression.NewLhsColumn(*identifier.NewColumnId("first_name")),
						expression.NewRhsExpr(
							expression.NewBinaryExpr(
								"=",
								expression.NewLhsColumn(*identifier.NewColumnId("last_name")),
								expression.NewRhsLiteral(literal.NewStringLiteral("'Doe'", "Doe")),
							),
						),
					),
				),
				expression.NewRhsExpr(
					expression.NewBinaryExpr(
						"=",
						expression.NewLhsColumn(*identifier.NewColumnId("id")),
						expression.NewRhsLiteral(literal.NewStringLiteral("'1'", "1")),
					),
				),
			),
		)
		search := NewSearchPlanner("users", where)

		// WHEN
		exec, err := search.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "when LHS is a column, RHS must be a literal")
	})

	t.Run("条件内で LHS が式、RHS がリテラルの場合、エラーを返す", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN: WHERE ((first_name = 'John') AND 'literal') のような不正な構造
		where := statement.NewWhereClause(
			expression.NewBinaryExpr(
				"AND",
				expression.NewLhsExpr(
					expression.NewBinaryExpr(
						"AND",
						expression.NewLhsExpr(
							expression.NewBinaryExpr(
								"=",
								expression.NewLhsColumn(*identifier.NewColumnId("first_name")),
								expression.NewRhsLiteral(literal.NewStringLiteral("'John'", "John")),
							),
						),
						expression.NewRhsLiteral(literal.NewStringLiteral("'invalid'", "invalid")),
					),
				),
				expression.NewRhsExpr(
					expression.NewBinaryExpr(
						"=",
						expression.NewLhsColumn(*identifier.NewColumnId("last_name")),
						expression.NewRhsLiteral(literal.NewStringLiteral("'Doe'", "Doe")),
					),
				),
			),
		)
		search := NewSearchPlanner("users", where)

		// WHEN
		exec, err := search.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "when LHS is an expression, RHS cannot be a literal")
	})
}

func TestComplexWhereWithData(t *testing.T) {
	// テストデータを挿入するヘルパー
	insertTestData := func(t *testing.T) {
		t.Helper()
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
	}

	// 検索結果を収集するヘルパー
	collectResults := func(t *testing.T, exec executor.Executor) []executor.Record {
		t.Helper()
		var results []executor.Record
		for {
			record, err := exec.Next()
			if err != nil {
				break
			}
			if len(record) == 0 {
				break
			}
			results = append(results, record)
		}
		return results
	}

	t.Run("AND 条件でデータをフィルタリングできる", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN
		insertTestData(t)

		// WHEN: (first_name = 'John') AND (last_name = 'Johnson')
		where := statement.NewWhereClause(
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
		)
		search := NewSearchPlanner("users", where)
		searchExec, err := search.Next()
		assert.NoError(t, err)

		// THEN: id=3 のレコードのみが返される
		results := collectResults(t, searchExec)
		assert.Equal(t, 1, len(results))
		assert.Equal(t, "3", string(results[0][0]))
		assert.Equal(t, "John", string(results[0][1]))
		assert.Equal(t, "Johnson", string(results[0][2]))
	})

	t.Run("OR 条件でデータをフィルタリングできる", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN
		insertTestData(t)

		// WHEN: (first_name = 'Jane') OR (last_name = 'Johnson')
		where := statement.NewWhereClause(
			expression.NewBinaryExpr(
				"OR",
				expression.NewLhsExpr(
					expression.NewBinaryExpr(
						"=",
						expression.NewLhsColumn(*identifier.NewColumnId("first_name")),
						expression.NewRhsLiteral(literal.NewStringLiteral("'Jane'", "Jane")),
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
		)
		search := NewSearchPlanner("users", where)
		searchExec, err := search.Next()
		assert.NoError(t, err)

		// THEN: id=2 (Jane/Smith) と id=3 (John/Johnson) が返される
		results := collectResults(t, searchExec)
		assert.Equal(t, 2, len(results))
		assert.Equal(t, "2", string(results[0][0]))
		assert.Equal(t, "Jane", string(results[0][1]))
		assert.Equal(t, "3", string(results[1][0]))
		assert.Equal(t, "John", string(results[1][1]))
	})

	t.Run("AND と OR の混合条件でデータをフィルタリングできる", func(t *testing.T) {
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer storage.ResetStorageManager()

		// GIVEN
		insertTestData(t)

		// WHEN: (last_name = 'Smith') OR ((first_name = 'John') AND (last_name = 'Doe'))
		// → id=1 (John/Doe) と id=2 (Jane/Smith) が該当
		where := statement.NewWhereClause(
			expression.NewBinaryExpr(
				"OR",
				expression.NewLhsExpr(
					expression.NewBinaryExpr(
						"=",
						expression.NewLhsColumn(*identifier.NewColumnId("last_name")),
						expression.NewRhsLiteral(literal.NewStringLiteral("'Smith'", "Smith")),
					),
				),
				expression.NewRhsExpr(
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
								expression.NewRhsLiteral(literal.NewStringLiteral("'Doe'", "Doe")),
							),
						),
					),
				),
			),
		)
		search := NewSearchPlanner("users", where)
		searchExec, err := search.Next()
		assert.NoError(t, err)

		// THEN: id=1 (John/Doe) と id=2 (Jane/Smith) が返される
		results := collectResults(t, searchExec)
		assert.Equal(t, 2, len(results))
		assert.Equal(t, "1", string(results[0][0]))
		assert.Equal(t, "John", string(results[0][1]))
		assert.Equal(t, "Doe", string(results[0][2]))
		assert.Equal(t, "2", string(results[1][0]))
		assert.Equal(t, "Jane", string(results[1][1]))
		assert.Equal(t, "Smith", string(results[1][2]))
	})
}

func TestOperatorToCondition(t *testing.T) {
	t.Run("= 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		record := executor.Record{[]byte("apple"), []byte("banana"), []byte("cherry")}

		// WHEN
		cond, err := operatorToCondition("=", 1, "banana")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cond)
		assert.True(t, cond(record))
		assert.False(t, cond(executor.Record{[]byte("apple"), []byte("orange"), []byte("cherry")}))
	})

	t.Run("!= 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		record := executor.Record{[]byte("apple"), []byte("banana"), []byte("cherry")}

		// WHEN
		cond, err := operatorToCondition("!=", 1, "banana")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cond)
		assert.False(t, cond(record))
		assert.True(t, cond(executor.Record{[]byte("apple"), []byte("orange"), []byte("cherry")}))
	})

	t.Run("< 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		// WHEN
		cond, err := operatorToCondition("<", 0, "c")

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
		// WHEN
		cond, err := operatorToCondition("<=", 0, "c")

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
		// WHEN
		cond, err := operatorToCondition(">", 0, "c")

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
		// WHEN
		cond, err := operatorToCondition(">=", 0, "c")

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
		// WHEN
		cond, err := operatorToCondition("LIKE", 0, "pattern")

		// THEN
		assert.Error(t, err)
		assert.Nil(t, cond)
		assert.Contains(t, err.Error(), "unsupported operator")
		assert.Contains(t, err.Error(), "LIKE")
	})

	t.Run("異なる position で条件が正しく適用される", func(t *testing.T) {
		// GIVEN
		record := executor.Record{[]byte("1"), []byte("John"), []byte("Doe")}

		// WHEN: position 0 (id)
		cond0, err0 := operatorToCondition("=", 0, "1")
		// WHEN: position 1 (first_name)
		cond1, err1 := operatorToCondition("=", 1, "John")
		// WHEN: position 2 (last_name)
		cond2, err2 := operatorToCondition("=", 2, "Doe")

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
