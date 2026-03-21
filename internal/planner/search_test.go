package planner

import (
	"minesql/internal/ast"
	"minesql/internal/catalog"
	"minesql/internal/engine"
	"minesql/internal/executor"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSearch(t *testing.T) {
	t.Run("WHERE 句なしの場合、SequentialScan が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		tblMeta := getTableMetadata(t, "users")
		search := NewSearch(tblMeta, nil)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.TableScan{}, exec)
	})

	t.Run("WHERE 句でインデックス付きカラムを指定した場合、IndexScan が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		tblMeta := getTableMetadata(t, "users")
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("last_name")),
				ast.NewRhsLiteral(ast.NewStringLiteral("'Doe'", "Doe")),
			),
			IsSet: true,
		}
		search := NewSearch(tblMeta, where)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.IndexScan{}, exec)
	})

	t.Run("WHERE 句でインデックスなしカラムを指定した場合、SequentialScan が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		tblMeta := getTableMetadata(t, "users")
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("first_name")),
				ast.NewRhsLiteral(ast.NewStringLiteral("'John'", "John")),
			),
			IsSet: true,
		}
		search := NewSearch(tblMeta, where)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.TableScan{}, exec)
	})

	t.Run("WHERE 句で存在しないカラムを指定した場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		tblMeta := getTableMetadata(t, "users")
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("non_existent_column")),
				ast.NewRhsLiteral(ast.NewStringLiteral("'value'", "value")),
			),
			IsSet: true,
		}
		search := NewSearch(tblMeta, where)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "does not exist in table")
	})

	t.Run("WHERE 句でサポートされていない型を指定した場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		tblMeta := getTableMetadata(t, "users")
		type UnsupportedExpr struct {
			ast.Expression
		}
		unsupported := &UnsupportedExpr{}
		where := &ast.WhereClause{
			Condition: unsupported,
			IsSet:     true,
		}
		search := NewSearch(tblMeta, where)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "unsupported WHERE condition type")
	})

	t.Run("複数の AND 条件で Filter が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		tblMeta := getTableMetadata(t, "users")
		// (id = '1') AND ((first_name = 'john') AND (last_name = 'doe'))
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"AND",
				ast.NewLhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("id")),
						ast.NewRhsLiteral(ast.NewStringLiteral("'1'", "1")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"AND",
						ast.NewLhsExpr(
							ast.NewBinaryExpr(
								"=",
								ast.NewLhsColumn(*ast.NewColumnId("first_name")),
								ast.NewRhsLiteral(ast.NewStringLiteral("'john'", "john")),
							),
						),
						ast.NewRhsExpr(
							ast.NewBinaryExpr(
								"=",
								ast.NewLhsColumn(*ast.NewColumnId("last_name")),
								ast.NewRhsLiteral(ast.NewStringLiteral("'doe'", "doe")),
							),
						),
					),
				),
			),
			IsSet: true,
		}
		search := NewSearch(tblMeta, where)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Filter{}, exec)
	})

	t.Run("AND と OR の混合条件で Filter が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		tblMeta := getTableMetadata(t, "users")
		// (first_name = 'John') OR ((id = '1') AND (last_name = 'Doe'))
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"OR",
				ast.NewLhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("first_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("'John'", "John")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"AND",
						ast.NewLhsExpr(
							ast.NewBinaryExpr(
								"=",
								ast.NewLhsColumn(*ast.NewColumnId("id")),
								ast.NewRhsLiteral(ast.NewStringLiteral("'1'", "1")),
							),
						),
						ast.NewRhsExpr(
							ast.NewBinaryExpr(
								"=",
								ast.NewLhsColumn(*ast.NewColumnId("last_name")),
								ast.NewRhsLiteral(ast.NewStringLiteral("'Doe'", "Doe")),
							),
						),
					),
				),
			),
			IsSet: true,
		}
		search := NewSearch(tblMeta, where)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Filter{}, exec)
	})

	t.Run("LHS がカラム、RHS が式の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		tblMeta := getTableMetadata(t, "users")
		// WHERE last_name = (first_name = 'John') のような不正な構造
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("last_name")),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("first_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("'John'", "John")),
					),
				),
			),
			IsSet: true,
		}
		search := NewSearch(tblMeta, where)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "when LHS is a column, RHS must be a literal")
	})

	t.Run("OR 演算子を使った場合、Filter が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		tblMeta := getTableMetadata(t, "users")
		// WHERE (first_name = 'John') OR (last_name = 'Doe')
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"OR",
				ast.NewLhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("first_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("'John'", "John")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("last_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("'Doe'", "Doe")),
					),
				),
			),
			IsSet: true,
		}
		search := NewSearch(tblMeta, where)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Filter{}, exec)
	})

	t.Run("条件内で存在しないカラムを指定した場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		tblMeta := getTableMetadata(t, "users")
		// WHERE (non_existent = 'value') AND (last_name = 'Doe')
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"AND",
				ast.NewLhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("non_existent")),
						ast.NewRhsLiteral(ast.NewStringLiteral("'value'", "value")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("last_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("'Doe'", "Doe")),
					),
				),
			),
			IsSet: true,
		}
		search := NewSearch(tblMeta, where)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "does not exist in table")
	})

	t.Run("条件内で LHS がカラム、RHS が式の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		tblMeta := getTableMetadata(t, "users")
		// WHERE (first_name = (last_name = 'Doe')) AND (id = '1')
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"AND",
				ast.NewLhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("first_name")),
						ast.NewRhsExpr(
							ast.NewBinaryExpr(
								"=",
								ast.NewLhsColumn(*ast.NewColumnId("last_name")),
								ast.NewRhsLiteral(ast.NewStringLiteral("'Doe'", "Doe")),
							),
						),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("id")),
						ast.NewRhsLiteral(ast.NewStringLiteral("'1'", "1")),
					),
				),
			),
			IsSet: true,
		}
		search := NewSearch(tblMeta, where)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "when LHS is a column, RHS must be a literal")
	})

	t.Run("条件内で LHS が式、RHS がリテラルの場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		tblMeta := getTableMetadata(t, "users")
		// WHERE ((first_name = 'John') AND 'literal') のような不正な構造
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"AND",
				ast.NewLhsExpr(
					ast.NewBinaryExpr(
						"AND",
						ast.NewLhsExpr(
							ast.NewBinaryExpr(
								"=",
								ast.NewLhsColumn(*ast.NewColumnId("first_name")),
								ast.NewRhsLiteral(ast.NewStringLiteral("'John'", "John")),
							),
						),
						ast.NewRhsLiteral(ast.NewStringLiteral("'invalid'", "invalid")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("last_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("'Doe'", "Doe")),
					),
				),
			),
			IsSet: true,
		}
		search := NewSearch(tblMeta, where)

		// WHEN
		exec, err := search.Build()

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
		insertStmt := &ast.InsertStmt{
			StmtType: ast.StmtTypeInsert,
			Table:    *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("first_name"),
				*ast.NewColumnId("last_name"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("'1'", "1"),
					ast.NewStringLiteral("'John'", "John"),
					ast.NewStringLiteral("'Doe'", "Doe"),
				},
				{
					ast.NewStringLiteral("'2'", "2"),
					ast.NewStringLiteral("'Jane'", "Jane"),
					ast.NewStringLiteral("'Smith'", "Smith"),
				},
				{
					ast.NewStringLiteral("'3'", "3"),
					ast.NewStringLiteral("'John'", "John"),
					ast.NewStringLiteral("'Johnson'", "Johnson"),
				},
			},
		}
		insertPlanner := NewInsert(insertStmt)
		insertExec, err := insertPlanner.Build()
		assert.NoError(t, err)
		_, err = insertExec.Next()
		assert.NoError(t, err)
	}

	// 検索結果を収集するヘルパー
	collectResults := func(t *testing.T, iter executor.Executor) []executor.Record {
		t.Helper()
		return fetchAll(t, iter)
	}

	t.Run("AND 条件でデータをフィルタリングできる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		insertTestData(t)

		tblMeta := getTableMetadata(t, "users")
		// (first_name = 'John') AND (last_name = 'Johnson')
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"AND",
				ast.NewLhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("first_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("'John'", "John")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("last_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("'Johnson'", "Johnson")),
					),
				),
			),
			IsSet: true,
		}
		search := NewSearch(tblMeta, where)
		searchExec, err := search.Build()
		assert.NoError(t, err)

		// WHEN
		results := collectResults(t, searchExec)

		// THEN: id=3 のレコードのみが返される
		assert.Equal(t, 1, len(results))
		assert.Equal(t, "3", string(results[0][0]))
		assert.Equal(t, "John", string(results[0][1]))
		assert.Equal(t, "Johnson", string(results[0][2]))
	})

	t.Run("OR 条件でデータをフィルタリングできる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		insertTestData(t)

		tblMeta := getTableMetadata(t, "users")
		// (first_name = 'Jane') OR (last_name = 'Johnson')
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"OR",
				ast.NewLhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("first_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("'Jane'", "Jane")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("last_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("'Johnson'", "Johnson")),
					),
				),
			),
			IsSet: true,
		}
		search := NewSearch(tblMeta, where)
		searchExec, err := search.Build()
		assert.NoError(t, err)

		// WHEN
		results := collectResults(t, searchExec)

		// THEN: id=2 (Jane/Smith) と id=3 (John/Johnson) が返される
		assert.Equal(t, 2, len(results))
		assert.Equal(t, "2", string(results[0][0]))
		assert.Equal(t, "Jane", string(results[0][1]))
		assert.Equal(t, "3", string(results[1][0]))
		assert.Equal(t, "John", string(results[1][1]))
	})

	t.Run("AND と OR の混合条件でデータをフィルタリングできる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		insertTestData(t)

		tblMeta := getTableMetadata(t, "users")
		// (last_name = 'Smith') OR ((first_name = 'John') AND (last_name = 'Doe'))
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"OR",
				ast.NewLhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("last_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("'Smith'", "Smith")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"AND",
						ast.NewLhsExpr(
							ast.NewBinaryExpr(
								"=",
								ast.NewLhsColumn(*ast.NewColumnId("first_name")),
								ast.NewRhsLiteral(ast.NewStringLiteral("'John'", "John")),
							),
						),
						ast.NewRhsExpr(
							ast.NewBinaryExpr(
								"=",
								ast.NewLhsColumn(*ast.NewColumnId("last_name")),
								ast.NewRhsLiteral(ast.NewStringLiteral("'Doe'", "Doe")),
							),
						),
					),
				),
			),
			IsSet: true,
		}
		search := NewSearch(tblMeta, where)
		searchExec, err := search.Build()
		assert.NoError(t, err)

		// WHEN
		results := collectResults(t, searchExec)

		// THEN: id=1 (John/Doe) と id=2 (Jane/Smith) が返される
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
	// operatorToCondition は Search のメソッドなので、ダミーの Search を使用する
	s := &Search{}

	t.Run("= 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		record := executor.Record{[]byte("apple"), []byte("banana"), []byte("cherry")}

		// WHEN
		cond, err := s.operatorToCondition("=", 1, "banana")

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
		cond, err := s.operatorToCondition("!=", 1, "banana")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cond)
		assert.False(t, cond(record))
		assert.True(t, cond(executor.Record{[]byte("apple"), []byte("orange"), []byte("cherry")}))
	})

	t.Run("< 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		// WHEN
		cond, err := s.operatorToCondition("<", 0, "c")

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
		cond, err := s.operatorToCondition("<=", 0, "c")

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
		cond, err := s.operatorToCondition(">", 0, "c")

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
		cond, err := s.operatorToCondition(">=", 0, "c")

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
		cond, err := s.operatorToCondition("LIKE", 0, "pattern")

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
		cond0, err0 := s.operatorToCondition("=", 0, "1")
		// WHEN: position 1 (first_name)
		cond1, err1 := s.operatorToCondition("=", 1, "John")
		// WHEN: position 2 (last_name)
		cond2, err2 := s.operatorToCondition("=", 2, "Doe")

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

	engine.Reset()
	engine.Init()
	engine.Get()

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

// テスト用にテーブルメタデータを取得する
//
//nolint:unparam // テーブル名は将来的に変わりうる
func getTableMetadata(t *testing.T, tableName string) *catalog.TableMetadata {
	t.Helper()
	e := engine.Get()
	tblMeta, ok := e.Catalog.GetTableMetadataByName(tableName)
	if !ok {
		t.Fatalf("table %s not found in catalog", tableName)
	}
	return tblMeta
}
