package planner

import (
	"testing"

	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"

	"github.com/stretchr/testify/assert"
)

func TestSearch(t *testing.T) {
	t.Run("WHERE 句なしの場合、SequentialScan が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		tblMeta := getTableMetadata(t, "users")
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, nil, handler.Get().BufferPool)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.TableScan{}, exec)
	})

	t.Run("WHERE 句でインデックス付きカラムを指定した場合、コストベースで適切なプランが生成される", func(t *testing.T) {
		// GIVEN: レコードが少ないテーブル → テーブルスキャンの方が安い → Filter が返る
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		tblMeta := getTableMetadata(t, "users")
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("last_name")),
				ast.NewRhsLiteral(ast.NewStringLiteral("Doe")),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

		// WHEN
		exec, err := search.Build()

		// THEN: コストベースでプランが決まるのでどちらかの型が返る
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		isIndexOrFilter := false
		switch exec.(type) {
		case *executor.IndexScan, *executor.Filter:
			isIndexOrFilter = true
		}
		assert.True(t, isIndexOrFilter, "expected IndexScan or Filter, got %T", exec)
	})

	t.Run("WHERE 句でインデックスなしカラムを指定した場合、Filter が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		tblMeta := getTableMetadata(t, "users")
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("first_name")),
				ast.NewRhsLiteral(ast.NewStringLiteral("John")),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

		// WHEN
		exec, err := search.Build()

		// THEN: テーブルスキャン + フィルタ
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Filter{}, exec)
	})

	t.Run("WHERE 句で存在しないカラムを指定した場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		tblMeta := getTableMetadata(t, "users")
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("non_existent_column")),
				ast.NewRhsLiteral(ast.NewStringLiteral("value")),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "does not exist in table")
	})

	t.Run("複数の AND 条件で Filter が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		tblMeta := getTableMetadata(t, "users")
		// (id = '1') AND ((first_name = 'john') AND (last_name = 'doe'))
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"AND",
				ast.NewLhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("id")),
						ast.NewRhsLiteral(ast.NewStringLiteral("1")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"AND",
						ast.NewLhsExpr(
							ast.NewBinaryExpr(
								"=",
								ast.NewLhsColumn(*ast.NewColumnId("first_name")),
								ast.NewRhsLiteral(ast.NewStringLiteral("john")),
							),
						),
						ast.NewRhsExpr(
							ast.NewBinaryExpr(
								"=",
								ast.NewLhsColumn(*ast.NewColumnId("last_name")),
								ast.NewRhsLiteral(ast.NewStringLiteral("doe")),
							),
						),
					),
				),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

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
		defer handler.Reset()

		tblMeta := getTableMetadata(t, "users")
		// (first_name = 'John') OR ((id = '1') AND (last_name = 'Doe'))
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"OR",
				ast.NewLhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("first_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("John")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"AND",
						ast.NewLhsExpr(
							ast.NewBinaryExpr(
								"=",
								ast.NewLhsColumn(*ast.NewColumnId("id")),
								ast.NewRhsLiteral(ast.NewStringLiteral("1")),
							),
						),
						ast.NewRhsExpr(
							ast.NewBinaryExpr(
								"=",
								ast.NewLhsColumn(*ast.NewColumnId("last_name")),
								ast.NewRhsLiteral(ast.NewStringLiteral("Doe")),
							),
						),
					),
				),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

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
		defer handler.Reset()

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
						ast.NewRhsLiteral(ast.NewStringLiteral("John")),
					),
				),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

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
		defer handler.Reset()

		tblMeta := getTableMetadata(t, "users")
		// WHERE (first_name = 'John') OR (last_name = 'Doe')
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"OR",
				ast.NewLhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("first_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("John")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("last_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("Doe")),
					),
				),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

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
		defer handler.Reset()

		tblMeta := getTableMetadata(t, "users")
		// WHERE (non_existent = 'value') AND (last_name = 'Doe')
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"AND",
				ast.NewLhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("non_existent")),
						ast.NewRhsLiteral(ast.NewStringLiteral("value")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("last_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("Doe")),
					),
				),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

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
		defer handler.Reset()

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
								ast.NewRhsLiteral(ast.NewStringLiteral("Doe")),
							),
						),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("id")),
						ast.NewRhsLiteral(ast.NewStringLiteral("1")),
					),
				),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "when LHS is a column, RHS must be a literal")
	})

	t.Run("条件内でサポートされていない論理演算子の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		tblMeta := getTableMetadata(t, "users")
		// WHERE (first_name = 'John') XOR (last_name = 'Doe')
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"XOR",
				ast.NewLhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("first_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("John")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("last_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("Doe")),
					),
				),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "unsupported logical operator")
	})

	t.Run("BinaryExpr で LHS がサポートされていない型の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		tblMeta := getTableMetadata(t, "users")

		type UnsupportedLHS struct {
			ast.LHS
		}
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				&UnsupportedLHS{},
				ast.NewRhsLiteral(ast.NewStringLiteral("value")),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

		// WHEN
		exec, err := search.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "unsupported LHS type")
	})

	t.Run("条件内で LHS が式、RHS がリテラルの場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

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
								ast.NewRhsLiteral(ast.NewStringLiteral("John")),
							),
						),
						ast.NewRhsLiteral(ast.NewStringLiteral("invalid")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("last_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("Doe")),
					),
				),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

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
		var trxId handler.TrxId = 1
		insertStmt := &ast.InsertStmt{
			Table: *ast.NewTableId("users"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("first_name"),
				*ast.NewColumnId("last_name"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("1"),
					ast.NewStringLiteral("John"),
					ast.NewStringLiteral("Doe"),
				},
				{
					ast.NewStringLiteral("2"),
					ast.NewStringLiteral("Jane"),
					ast.NewStringLiteral("Smith"),
				},
				{
					ast.NewStringLiteral("3"),
					ast.NewStringLiteral("John"),
					ast.NewStringLiteral("Johnson"),
				},
			},
		}
		insertExec, err := PlanInsert(trxId, insertStmt)
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
		defer handler.Reset()

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
						ast.NewRhsLiteral(ast.NewStringLiteral("John")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("last_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("Johnson")),
					),
				),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)
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
		defer handler.Reset()

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
						ast.NewRhsLiteral(ast.NewStringLiteral("Jane")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(*ast.NewColumnId("last_name")),
						ast.NewRhsLiteral(ast.NewStringLiteral("Johnson")),
					),
				),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)
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

	t.Run("範囲演算子でデータをフィルタリングできる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		insertTestData(t)

		tblMeta := getTableMetadata(t, "users")
		// WHERE id > '1'
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				">",
				ast.NewLhsColumn(*ast.NewColumnId("id")),
				ast.NewRhsLiteral(ast.NewStringLiteral("1")),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)
		searchExec, err := search.Build()
		assert.NoError(t, err)

		// WHEN
		results := collectResults(t, searchExec)

		// THEN: id=2 と id=3 が返される (id > '1')
		assert.Equal(t, 2, len(results))
		assert.Equal(t, "2", string(results[0][0]))
		assert.Equal(t, "3", string(results[1][0]))
	})

	t.Run("AND と OR の混合条件でデータをフィルタリングできる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

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
						ast.NewRhsLiteral(ast.NewStringLiteral("Smith")),
					),
				),
				ast.NewRhsExpr(
					ast.NewBinaryExpr(
						"AND",
						ast.NewLhsExpr(
							ast.NewBinaryExpr(
								"=",
								ast.NewLhsColumn(*ast.NewColumnId("first_name")),
								ast.NewRhsLiteral(ast.NewStringLiteral("John")),
							),
						),
						ast.NewRhsExpr(
							ast.NewBinaryExpr(
								"=",
								ast.NewLhsColumn(*ast.NewColumnId("last_name")),
								ast.NewRhsLiteral(ast.NewStringLiteral("Doe")),
							),
						),
					),
				),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)
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

// テスト用の storage manager を初期化
func initStorageManager(t *testing.T, dataDir string) {
	t.Setenv("MINESQL_DATA_DIR", dataDir)
	t.Setenv("MINESQL_BUFFER_SIZE", "10")

	handler.Reset()
	handler.Init()
	handler.Get()

	// テーブルを作成
	createTable := executor.NewCreateTable("users", 1, []handler.CreateIndexParam{
		{Name: "last_name", ColName: "last_name", UkIdx: 2},
	}, []handler.CreateColumnParam{
		{Name: "id", Type: handler.ColumnTypeString},
		{Name: "first_name", Type: handler.ColumnTypeString},
		{Name: "last_name", Type: handler.ColumnTypeString},
	})
	_, err := createTable.Next()
	assert.NoError(t, err)
}

func TestPlanSelection(t *testing.T) {
	t.Run("PK の = 検索ではユニークスキャンが選ばれ TableScan が返される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		tblMeta := getTableMetadata(t, "users")
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("id")),
				ast.NewRhsLiteral(ast.NewStringLiteral("1")),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

		// WHEN
		exec, err := search.Build()

		// THEN: PK = 検索 → ユニークスキャン (コスト 1.0) → TableScan が選ばれる
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.TableScan{}, exec)
	})

	t.Run("UNIQUE INDEX の = 検索ではユニークスキャンが選ばれ IndexScan が返される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		tblMeta := getTableMetadata(t, "users")
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("last_name")),
				ast.NewRhsLiteral(ast.NewStringLiteral("Doe")),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

		// WHEN
		exec, err := search.Build()

		// THEN: UNIQUE INDEX = 検索 → ユニークスキャン (コスト 1.0) → IndexScan が選ばれる
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.IndexScan{}, exec)
	})

	t.Run("!= 検索ではフルスキャン + Filter が選ばれる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		tblMeta := getTableMetadata(t, "users")
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"!=",
				ast.NewLhsColumn(*ast.NewColumnId("id")),
				ast.NewRhsLiteral(ast.NewStringLiteral("1")),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

		// WHEN
		exec, err := search.Build()

		// THEN: != はレンジ分析対象外 → フルスキャン + Filter
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Filter{}, exec)
	})

	t.Run("インデックスなしカラムの = 検索ではフルスキャン + Filter が選ばれる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		tblMeta := getTableMetadata(t, "users")
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("first_name")),
				ast.NewRhsLiteral(ast.NewStringLiteral("John")),
			),
		}
		search := NewSearch(access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), tblMeta, where, handler.Get().BufferPool)

		// WHEN
		exec, err := search.Build()

		// THEN: first_name にインデックスがない → フルスキャン + Filter
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Filter{}, exec)
	})
}

func TestCostFormulas(t *testing.T) {
	t.Run("フルスキャンのコストが cost.md の式と一致する", func(t *testing.T) {
		// GIVEN: cost.md の計算例
		stats := &handler.TableStatistics{
			RecordCount:   74822,
			LeafPageCount: 1924,
		}
		pageReadCost := 1.0

		// WHEN
		readCost := calcFullScanReadCost(stats, pageReadCost)
		totalCost := calcFullScanCost(stats, pageReadCost)

		// THEN: cost.md の式: scanTime × pageReadCost + foundRecords × RowEvaluateCost
		assert.Equal(t, 1924.0, readCost)              // scanTime × 1.0
		assert.Equal(t, 1924.0+74822.0*0.1, totalCost) // 9406.2
	})

	t.Run("ユニークスキャンのコストが 1.0 固定である", func(t *testing.T) {
		// WHEN
		cost := calcUniqueScanCost()

		// THEN
		assert.Equal(t, 1.0, cost)
	})

	t.Run("レンジスキャンのコストが cost.md の 2 段階計算と一致する", func(t *testing.T) {
		// GIVEN: cost.md の計算例 (セカンダリインデックス)
		foundRecords := 500.0
		nRanges := 1
		pageReadCost := 1.0

		// WHEN
		readTime := calcReadTimeForSecondaryIndex(nRanges, foundRecords, pageReadCost)
		totalCost := calcRangeScanCost(readTime, foundRecords)

		// THEN: readTime = (1 + 500) × 1.0 = 501
		//       rangeCost = 501 + 500 × 0.1 + 0.01 = 551.01
		//       totalCost = 551.01 + 500 × 0.1 = 601.01
		assert.Equal(t, 501.0, readTime)
		assert.Equal(t, 601.01, totalCost)
	})

	t.Run("クラスタ化インデックスの readTime が foundRecords に応じて分岐する", func(t *testing.T) {
		// GIVEN
		pageReadCost := 1.0

		// WHEN: foundRecords <= 2
		readTime1 := calcReadTimeForClusteredIndex(1, 2, 10000, 100, pageReadCost)

		// WHEN: foundRecords > 2
		readTime500 := calcReadTimeForClusteredIndex(1, 500, 10000, 100, pageReadCost)

		// THEN
		assert.Equal(t, 2.0, readTime1)   // foundRecords × pageReadCost
		assert.Equal(t, 6.0, readTime500) // (1 + 500/10000 × 100) × 1.0
	})

	t.Run("フルスキャンよりユニークスキャンの方がコストが低い", func(t *testing.T) {
		// GIVEN: 少数でもテーブルがあればフルスキャン > 1.0
		stats := &handler.TableStatistics{
			RecordCount:   10,
			LeafPageCount: 1,
		}
		pageReadCost := 1.0

		// WHEN
		fullCost := calcFullScanCost(stats, pageReadCost)
		uniqueCost := calcUniqueScanCost()

		// THEN: フルスキャン = 1.0 + 10 × 0.1 = 2.0 > ユニーク = 1.0
		assert.Greater(t, fullCost, uniqueCost)
	})
}

func TestIsPKLeadingColumn(t *testing.T) {
	t.Run("単一カラム PK の先頭カラムで true を返す", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		tblMeta := getTableMetadata(t, "users")
		s := &Search{tblMeta: tblMeta}

		// WHEN & THEN
		assert.True(t, s.isPKLeadingColumn("id"))
	})

	t.Run("PK でないカラムで false を返す", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		tblMeta := getTableMetadata(t, "users")
		s := &Search{tblMeta: tblMeta}

		// WHEN & THEN
		assert.False(t, s.isPKLeadingColumn("first_name"))
	})

	t.Run("存在しないカラムで false を返す", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		tblMeta := getTableMetadata(t, "users")
		s := &Search{tblMeta: tblMeta}

		// WHEN & THEN
		assert.False(t, s.isPKLeadingColumn("nonexistent"))
	})

	t.Run("複合 PK の先頭カラムで false を返す", func(t *testing.T) {
		// GIVEN: 複合 PK (id, name) のテーブル
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		executePlan(t, &ast.CreateTableStmt{
			TableName: "composite_pk",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "name", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "val", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
					*ast.NewColumnId("name"),
				}},
			},
		})

		tblMeta := getTableMetadata(t, "composite_pk")
		s := &Search{tblMeta: tblMeta}

		// WHEN & THEN: 複合 PK では先頭カラムだけでは一意にならないため false
		assert.False(t, s.isPKLeadingColumn("id"))
		assert.False(t, s.isPKLeadingColumn("name"))
	})
}

// テスト用にテーブルメタデータを取得する
//
//nolint:unparam // テーブル名は将来的に変わりうる
func getTableMetadata(t *testing.T, tableName string) *handler.TableMetadata {
	t.Helper()
	hdl := handler.Get()
	tblMeta, ok := hdl.Catalog.GetTableMetaByName(tableName)
	if !ok {
		t.Fatalf("table %s not found in catalog", tableName)
	}
	return tblMeta
}
