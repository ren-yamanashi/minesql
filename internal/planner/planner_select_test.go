package planner

import (
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/dictionary"
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanSelect(t *testing.T) {
	t.Run("指定したテーブルが存在しない場合にエラーになる", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		stmt := &ast.SelectStmt{From: *ast.NewTableId("non_existent_table"), Where: nil}

		// WHEN
		exec, err := PlanSelect(0, stmt)

		// THEN
		assert.Nil(t, exec)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non_existent_table")
	})

	t.Run("Build で Project Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
		})

		stmt := &ast.SelectStmt{
			From:  *ast.NewTableId("users"),
			Where: nil,
		}

		// WHEN
		exec, err := PlanSelect(0, stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Project{}, exec)
	})

	t.Run("Project が全カラムの位置を保持する", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
			{Name: "email", Type: handler.ColumnTypeString},
		})

		stmt := &ast.SelectStmt{
			From:  *ast.NewTableId("users"),
			Where: nil,
		}

		// WHEN
		exec, err := PlanSelect(0, stmt)

		// THEN
		assert.NoError(t, err)
		proj, ok := exec.(*executor.Project)
		assert.True(t, ok)
		assert.Equal(t, []uint16{0, 1, 2}, proj.ColPos)
	})

	t.Run("WHERE句に条件が指定されている場合、Project Executorが生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
		})

		stmt := &ast.SelectStmt{
			From: *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("id")),
					ast.NewRhsLiteral(ast.NewStringLiteral("1")),
				),
			},
		}

		// WHEN
		exec, err := PlanSelect(0, stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Project{}, exec)
	})

	t.Run("WHERE句に存在しないカラムが指定された場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
		})

		stmt := &ast.SelectStmt{
			From: *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("non_existent")),
					ast.NewRhsLiteral(ast.NewStringLiteral("test")),
				),
			},
		}

		// WHEN
		exec, err := PlanSelect(0, stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "non_existent")
	})

	t.Run("Project 経由でレコードを取得できる", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
		})

		// データを挿入
		executePlan(t, &ast.InsertStmt{
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
			},
		})

		stmt := &ast.SelectStmt{
			From:  *ast.NewTableId("users"),
			Where: nil,
		}

		// WHEN
		hdl := handler.Get()
		trxId := hdl.BeginTrx()
		exec, err := PlanSelect(trxId, stmt)
		assert.NoError(t, err)
		results := fetchAll(t, exec)
		assert.NoError(t, hdl.CommitTrx(trxId))

		// THEN
		assert.Equal(t, 2, len(results))
		assert.Equal(t, executor.Record{[]byte("1"), []byte("Alice")}, results[0])
		assert.Equal(t, executor.Record{[]byte("2"), []byte("Bob")}, results[1])
	})
}

func TestSplitWhereForTable(t *testing.T) {
	usersMeta := &handler.TableMetadata{
		Name: "users", NCols: 2, PKCount: 1,
		Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}, {Name: "name", Pos: 1}},
	}
	ordersMeta := &handler.TableMetadata{
		Name: "orders", NCols: 2, PKCount: 1,
		Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}, {Name: "user_id", Pos: 1}},
	}
	allTables := []*handler.TableMetadata{usersMeta, ordersMeta}

	t.Run("駆動表のカラムのみの条件が pushdown される", func(t *testing.T) {
		// GIVEN: WHERE users.id = '1'
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr("=",
				ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "id"}),
				ast.NewRhsLiteral(ast.NewStringLiteral("1")),
			),
		}

		// WHEN
		forTable, remaining := splitWhereForTable(where, usersMeta, allTables)

		// THEN
		require.NotNil(t, forTable)
		assert.Nil(t, remaining)
		assert.Equal(t, "=", forTable.Condition.Operator)
	})

	t.Run("他テーブルの条件は pushdown されず remaining に残る", func(t *testing.T) {
		// GIVEN: WHERE orders.user_id = '1'
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr("=",
				ast.NewLhsColumn(ast.ColumnId{TableName: "orders", ColName: "user_id"}),
				ast.NewRhsLiteral(ast.NewStringLiteral("1")),
			),
		}

		// WHEN
		forTable, remaining := splitWhereForTable(where, usersMeta, allTables)

		// THEN
		assert.Nil(t, forTable)
		require.NotNil(t, remaining)
	})

	t.Run("AND の複合条件が正しく分離される", func(t *testing.T) {
		// GIVEN: WHERE users.id = '1' AND orders.user_id = '1'
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr("AND",
				ast.NewLhsExpr(ast.NewBinaryExpr("=",
					ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "id"}),
					ast.NewRhsLiteral(ast.NewStringLiteral("1")),
				)),
				ast.NewRhsExpr(ast.NewBinaryExpr("=",
					ast.NewLhsColumn(ast.ColumnId{TableName: "orders", ColName: "user_id"}),
					ast.NewRhsLiteral(ast.NewStringLiteral("1")),
				)),
			),
		}

		// WHEN
		forTable, remaining := splitWhereForTable(where, usersMeta, allTables)

		// THEN: users.id = '1' が pushdown、orders.user_id = '1' が remaining
		require.NotNil(t, forTable)
		require.NotNil(t, remaining)
	})

	t.Run("WHERE が nil の場合は両方 nil", func(t *testing.T) {
		// WHEN
		forTable, remaining := splitWhereForTable(nil, usersMeta, allTables)

		// THEN
		assert.Nil(t, forTable)
		assert.Nil(t, remaining)
	})

	t.Run("非修飾名で同名カラムが複数テーブルにある場合は pushdown されない", func(t *testing.T) {
		// GIVEN: 両テーブルに "id" カラムがある場合、WHERE id = '1' は曖昧
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr("=",
				ast.NewLhsColumn(ast.ColumnId{ColName: "id"}), // 非修飾名
				ast.NewRhsLiteral(ast.NewStringLiteral("1")),
			),
		}

		// WHEN
		forTable, remaining := splitWhereForTable(where, usersMeta, allTables)

		// THEN: 曖昧なので pushdown されず remaining に残る
		assert.Nil(t, forTable)
		require.NotNil(t, remaining)
	})

	t.Run("非修飾名で片方のテーブルにしかないカラムは pushdown される", func(t *testing.T) {
		// GIVEN: "name" は users にのみ存在、"user_id" は orders にのみ存在
		where := &ast.WhereClause{
			Condition: ast.NewBinaryExpr("=",
				ast.NewLhsColumn(ast.ColumnId{ColName: "name"}), // 非修飾名、users のみ
				ast.NewRhsLiteral(ast.NewStringLiteral("Alice")),
			),
		}

		// WHEN
		forTable, remaining := splitWhereForTable(where, usersMeta, allTables)

		// THEN: users にのみ存在するので pushdown される
		require.NotNil(t, forTable)
		assert.Nil(t, remaining)
	})
}

func TestCollectTableNames(t *testing.T) {
	t.Run("FROM のみの場合は 1 テーブル", func(t *testing.T) {
		// GIVEN
		stmt := &ast.SelectStmt{From: *ast.NewTableId("users")}

		// WHEN
		names := collectTableNames(stmt)

		// THEN
		assert.Equal(t, []string{"users"}, names)
	})

	t.Run("JOIN がある場合は FROM + JOIN テーブル", func(t *testing.T) {
		// GIVEN
		stmt := &ast.SelectStmt{
			From: *ast.NewTableId("users"),
			Joins: []*ast.JoinClause{
				{Table: *ast.NewTableId("orders")},
				{Table: *ast.NewTableId("items")},
			},
		}

		// WHEN
		names := collectTableNames(stmt)

		// THEN
		assert.Equal(t, []string{"users", "orders", "items"}, names)
	})
}

func TestExtractPredicatesFromExpr(t *testing.T) {
	t.Run("単一の等値条件から joinPredicate が抽出される", func(t *testing.T) {
		// GIVEN: users.id = orders.user_id
		expr := ast.NewBinaryExpr("=",
			ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "id"}),
			ast.NewRhsColumn(ast.ColumnId{TableName: "orders", ColName: "user_id"}),
		)

		// WHEN
		preds, err := extractPredicatesFromExpr(expr)

		// THEN
		require.NoError(t, err)
		require.Len(t, preds, 1)
		assert.Equal(t, "users", preds[0].leftTable)
		assert.Equal(t, "id", preds[0].leftCol)
		assert.Equal(t, "orders", preds[0].rightTable)
		assert.Equal(t, "user_id", preds[0].rightCol)
	})

	t.Run("AND で複数条件が結合されている場合に全て抽出される", func(t *testing.T) {
		// GIVEN: (users.id = orders.user_id) AND (orders.item_id = items.id)
		expr := ast.NewBinaryExpr("AND",
			ast.NewLhsExpr(ast.NewBinaryExpr("=",
				ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "id"}),
				ast.NewRhsColumn(ast.ColumnId{TableName: "orders", ColName: "user_id"}),
			)),
			ast.NewRhsExpr(ast.NewBinaryExpr("=",
				ast.NewLhsColumn(ast.ColumnId{TableName: "orders", ColName: "item_id"}),
				ast.NewRhsColumn(ast.ColumnId{TableName: "items", ColName: "id"}),
			)),
		)

		// WHEN
		preds, err := extractPredicatesFromExpr(expr)

		// THEN
		require.NoError(t, err)
		require.Len(t, preds, 2)
		assert.Equal(t, "users", preds[0].leftTable)
		assert.Equal(t, "orders", preds[1].leftTable)
	})

	t.Run("nil の場合は空スライスが返される", func(t *testing.T) {
		// WHEN
		preds, err := extractPredicatesFromExpr(nil)

		// THEN
		require.NoError(t, err)
		assert.Nil(t, preds)
	})

	t.Run("サポートされていない構造でエラーになる", func(t *testing.T) {
		// GIVEN: col = literal (結合条件ではない)
		expr := ast.NewBinaryExpr("=",
			ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "id"}),
			ast.NewRhsLiteral(ast.NewStringLiteral("1")),
		)

		// WHEN
		_, err := extractPredicatesFromExpr(expr)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported ON condition")
	})
}

func TestCombineExprsWithAND(t *testing.T) {
	t.Run("1 つの式はそのまま返される", func(t *testing.T) {
		// GIVEN
		expr := ast.NewBinaryExpr("=",
			ast.NewLhsColumn(ast.ColumnId{ColName: "id"}),
			ast.NewRhsLiteral(ast.NewStringLiteral("1")),
		)

		// WHEN
		result := combineExprsWithAND([]*ast.BinaryExpr{expr})

		// THEN: そのまま返される (ポインタ同一)
		assert.Same(t, expr, result)
	})

	t.Run("複数の式が AND で結合される", func(t *testing.T) {
		// GIVEN
		expr1 := ast.NewBinaryExpr("=",
			ast.NewLhsColumn(ast.ColumnId{ColName: "id"}),
			ast.NewRhsLiteral(ast.NewStringLiteral("1")),
		)
		expr2 := ast.NewBinaryExpr("=",
			ast.NewLhsColumn(ast.ColumnId{ColName: "name"}),
			ast.NewRhsLiteral(ast.NewStringLiteral("Alice")),
		)
		expr3 := ast.NewBinaryExpr(">",
			ast.NewLhsColumn(ast.ColumnId{ColName: "age"}),
			ast.NewRhsLiteral(ast.NewStringLiteral("20")),
		)

		// WHEN
		result := combineExprsWithAND([]*ast.BinaryExpr{expr1, expr2, expr3})

		// THEN: 結果は AND ツリー
		assert.Equal(t, "AND", result.Operator)

		// 左辺も AND (expr1 AND expr2)
		lhs, ok := result.Left.(*ast.LhsExpr)
		require.True(t, ok)
		assert.Equal(t, "AND", lhs.Expr.Operator)

		// 右辺は expr3
		rhs, ok := result.Right.(*ast.RhsExpr)
		require.True(t, ok)
		assert.Equal(t, ">", rhs.Expr.Operator)
	})
}
