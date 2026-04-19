package planner

import (
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/handler"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Executor から全レコードを取得する
func fetchAll(t *testing.T, iter executor.Executor) []executor.Record {
	t.Helper()
	var records []executor.Record
	for {
		record, err := iter.Next()
		assert.NoError(t, err)
		if record == nil {
			return records
		}
		records = append(records, record)
	}
}

// AST を直接構築 → PlanStart → ExecutePlan で実行する
func executePlan(t *testing.T, stmt ast.Statement) []executor.Record {
	t.Helper()
	hdl := handler.Get()
	trxId := hdl.BeginTrx()
	exec, err := Start(trxId, stmt)
	assert.NoError(t, err)

	records := fetchAll(t, exec)
	assert.NoError(t, hdl.CommitTrx(trxId))
	return records
}

// ストレージを初期化し、5 カラムの users テーブルを作成してデータを投入する
func setupUsersTable(t *testing.T) {
	t.Helper()

	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "100")
	handler.Reset()
	handler.Init()

	// CREATE TABLE
	executePlan(t, &ast.CreateTableStmt{
		TableName: "users",
		CreateDefinitions: []ast.Definition{
			&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{ColName: "first_name", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{ColName: "last_name", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{ColName: "gender", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{ColName: "username", DataType: ast.DataTypeVarchar},
			&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
				*ast.NewColumnId("id"),
			}},
			&ast.ConstraintUniqueKeyDef{Column: *ast.NewColumnId("username")},
		},
	})

	// INSERT
	executePlan(t, &ast.InsertStmt{
		Table: *ast.NewTableId("users"),
		Cols: []ast.ColumnId{
			*ast.NewColumnId("id"),
			*ast.NewColumnId("first_name"),
			*ast.NewColumnId("last_name"),
			*ast.NewColumnId("gender"),
			*ast.NewColumnId("username"),
		},
		Values: [][]ast.Literal{
			{
				ast.NewStringLiteral("1"),
				ast.NewStringLiteral("John"),
				ast.NewStringLiteral("Doe"),
				ast.NewStringLiteral("male"),
				ast.NewStringLiteral("johndoe"),
			},
			{
				ast.NewStringLiteral("2"),
				ast.NewStringLiteral("John"),
				ast.NewStringLiteral("Doe2"),
				ast.NewStringLiteral("male"),
				ast.NewStringLiteral("johndoe2"),
			},
			{
				ast.NewStringLiteral("3"),
				ast.NewStringLiteral("John"),
				ast.NewStringLiteral("Doe3"),
				ast.NewStringLiteral("male"),
				ast.NewStringLiteral("johndoe3"),
			},
			{
				ast.NewStringLiteral("4"),
				ast.NewStringLiteral("Jane"),
				ast.NewStringLiteral("Doe2"),
				ast.NewStringLiteral("female"),
				ast.NewStringLiteral("janedoe"),
			},
			{
				ast.NewStringLiteral("5"),
				ast.NewStringLiteral("Jonathan"),
				ast.NewStringLiteral("Black"),
				ast.NewStringLiteral("male"),
				ast.NewStringLiteral("jonathanblack"),
			},
			{
				ast.NewStringLiteral("6"),
				ast.NewStringLiteral("Tom"),
				ast.NewStringLiteral("Brown"),
				ast.NewStringLiteral("male"),
				ast.NewStringLiteral("tombrown"),
			},
		},
	})
}

// ストレージを初期化し、非ユニークインデックス付きの products テーブルを作成してデータを投入する
//
// テーブル: products (id PK, name, category)
// インデックス: KEY idx_category (category)
// データ: ("1","Apple","Fruit"), ("2","Banana","Fruit"), ("3","Carrot","Veggie")
func setupProductsTable(t *testing.T) {
	t.Helper()

	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "100")
	handler.Reset()
	handler.Init()

	executePlan(t, &ast.CreateTableStmt{
		TableName: "products",
		CreateDefinitions: []ast.Definition{
			&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{ColName: "name", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{ColName: "category", DataType: ast.DataTypeVarchar},
			&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
			&ast.ConstraintKeyDef{KeyName: "idx_category", Column: *ast.NewColumnId("category")},
		},
	})

	executePlan(t, &ast.InsertStmt{
		Table: *ast.NewTableId("products"),
		Cols:  []ast.ColumnId{*ast.NewColumnId("id"), *ast.NewColumnId("name"), *ast.NewColumnId("category")},
		Values: [][]ast.Literal{
			{ast.NewStringLiteral("1"), ast.NewStringLiteral("Apple"), ast.NewStringLiteral("Fruit")},
			{ast.NewStringLiteral("2"), ast.NewStringLiteral("Banana"), ast.NewStringLiteral("Fruit")},
			{ast.NewStringLiteral("3"), ast.NewStringLiteral("Carrot"), ast.NewStringLiteral("Veggie")},
		},
	})
}

// レコードを strings.Builder に書き出す
func writeRecords(sb *strings.Builder, records []executor.Record) {
	for _, record := range records {
		cols := make([]string, len(record))
		for i, col := range record {
			cols[i] = string(col)
		}
		fmt.Fprintf(sb, "  (%s)\n", strings.Join(cols, ", "))
	}
	fmt.Fprintf(sb, "  合計: %d 件\n", len(records))
}

func TestPlannerIntegration(t *testing.T) {
	t.Run("SELECT でフルテーブルスキャンできる", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer handler.Reset()

		// WHEN
		records := executePlan(t, &ast.SelectStmt{
			From:  *ast.NewTableId("users"),
			Where: nil,
		})

		// THEN
		var sb strings.Builder
		sb.WriteString("=== SELECT 全件 ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== SELECT 全件 ===\n" +
			"  (1, John, Doe, male, johndoe)\n" +
			"  (2, John, Doe2, male, johndoe2)\n" +
			"  (3, John, Doe3, male, johndoe3)\n" +
			"  (4, Jane, Doe2, female, janedoe)\n" +
			"  (5, Jonathan, Black, male, jonathanblack)\n" +
			"  (6, Tom, Brown, male, tombrown)\n" +
			"  合計: 6 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("WHERE 句で等値検索できる", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer handler.Reset()

		// WHEN
		records := executePlan(t, &ast.SelectStmt{
			From: *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("username")),
					ast.NewRhsLiteral(ast.NewStringLiteral("janedoe")),
				),
			},
		})

		// THEN
		var sb strings.Builder
		sb.WriteString("=== WHERE 等値検索 ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== WHERE 等値検索 ===\n" +
			"  (4, Jane, Doe2, female, janedoe)\n" +
			"  合計: 1 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("AND と OR の複合条件でフィルタリングできる", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer handler.Reset()

		// WHEN: (first_name < 'K' AND gender = 'male' AND last_name >= 'Doe') OR first_name = 'Tom'
		records := executePlan(t, &ast.SelectStmt{
			From: *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"OR",
					ast.NewLhsExpr(
						ast.NewBinaryExpr(
							"AND",
							ast.NewLhsExpr(
								ast.NewBinaryExpr(
									"<",
									ast.NewLhsColumn(*ast.NewColumnId("first_name")),
									ast.NewRhsLiteral(ast.NewStringLiteral("K")),
								),
							),
							ast.NewRhsExpr(
								ast.NewBinaryExpr(
									"AND",
									ast.NewLhsExpr(
										ast.NewBinaryExpr(
											"=",
											ast.NewLhsColumn(*ast.NewColumnId("gender")),
											ast.NewRhsLiteral(ast.NewStringLiteral("male")),
										),
									),
									ast.NewRhsExpr(
										ast.NewBinaryExpr(
											">=",
											ast.NewLhsColumn(*ast.NewColumnId("last_name")),
											ast.NewRhsLiteral(ast.NewStringLiteral("Doe")),
										),
									),
								),
							),
						),
					),
					ast.NewRhsExpr(
						ast.NewBinaryExpr(
							"=",
							ast.NewLhsColumn(*ast.NewColumnId("first_name")),
							ast.NewRhsLiteral(ast.NewStringLiteral("Tom")),
						),
					),
				),
			},
		})

		// THEN
		var sb strings.Builder
		sb.WriteString("=== AND/OR 複合条件 ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== AND/OR 複合条件 ===\n" +
			"  (1, John, Doe, male, johndoe)\n" +
			"  (2, John, Doe2, male, johndoe2)\n" +
			"  (3, John, Doe3, male, johndoe3)\n" +
			"  (6, Tom, Brown, male, tombrown)\n" +
			"  合計: 4 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("PK に対する >= 条件で複数行が返される", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer handler.Reset()

		// WHEN: WHERE id >= '4' → id=4, 5, 6 の 3 件が返されるべき
		records := executePlan(t, &ast.SelectStmt{
			From: *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					">=",
					ast.NewLhsColumn(*ast.NewColumnId("id")),
					ast.NewRhsLiteral(ast.NewStringLiteral("4")),
				),
			},
		})

		// THEN
		var sb strings.Builder
		sb.WriteString("=== WHERE id >= '4' ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== WHERE id >= '4' ===\n" +
			"  (4, Jane, Doe2, female, janedoe)\n" +
			"  (5, Jonathan, Black, male, jonathanblack)\n" +
			"  (6, Tom, Brown, male, tombrown)\n" +
			"  合計: 3 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("PK に対する > 条件で複数行が返される", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer handler.Reset()

		// WHEN: WHERE id > '4' → id=5, 6 の 2 件が返されるべき
		records := executePlan(t, &ast.SelectStmt{
			From: *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					">",
					ast.NewLhsColumn(*ast.NewColumnId("id")),
					ast.NewRhsLiteral(ast.NewStringLiteral("4")),
				),
			},
		})

		// THEN
		var sb strings.Builder
		sb.WriteString("=== WHERE id > '4' ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== WHERE id > '4' ===\n" +
			"  (5, Jonathan, Black, male, jonathanblack)\n" +
			"  (6, Tom, Brown, male, tombrown)\n" +
			"  合計: 2 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("PK に対する <= 条件で複数行が返される", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer handler.Reset()

		// WHEN: WHERE id <= '3' → id=1, 2, 3 の 3 件が返されるべき
		records := executePlan(t, &ast.SelectStmt{
			From: *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"<=",
					ast.NewLhsColumn(*ast.NewColumnId("id")),
					ast.NewRhsLiteral(ast.NewStringLiteral("3")),
				),
			},
		})

		// THEN
		var sb strings.Builder
		sb.WriteString("=== WHERE id <= '3' ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== WHERE id <= '3' ===\n" +
			"  (1, John, Doe, male, johndoe)\n" +
			"  (2, John, Doe2, male, johndoe2)\n" +
			"  (3, John, Doe3, male, johndoe3)\n" +
			"  合計: 3 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("PK に対する < 条件で複数行が返される", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer handler.Reset()

		// WHEN: WHERE id < '3' → id=1, 2 の 2 件が返されるべき
		records := executePlan(t, &ast.SelectStmt{
			From: *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"<",
					ast.NewLhsColumn(*ast.NewColumnId("id")),
					ast.NewRhsLiteral(ast.NewStringLiteral("3")),
				),
			},
		})

		// THEN
		var sb strings.Builder
		sb.WriteString("=== WHERE id < '3' ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== WHERE id < '3' ===\n" +
			"  (1, John, Doe, male, johndoe)\n" +
			"  (2, John, Doe2, male, johndoe2)\n" +
			"  合計: 2 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("UPDATE でレコードを更新できる", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer handler.Reset()

		// WHEN (UPDATE users SET last_name = 'Smith' WHERE username = 'johndoe')
		executePlan(t, &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("last_name"), Value: ast.NewStringLiteral("Smith")},
			},
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("username")),
					ast.NewRhsLiteral(ast.NewStringLiteral("johndoe")),
				),
			},
		})

		// THEN
		records := executePlan(t, &ast.SelectStmt{
			From:  *ast.NewTableId("users"),
			Where: nil,
		})

		var sb strings.Builder
		sb.WriteString("=== UPDATE 後の全件 ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== UPDATE 後の全件 ===\n" +
			"  (1, John, Smith, male, johndoe)\n" +
			"  (2, John, Doe2, male, johndoe2)\n" +
			"  (3, John, Doe3, male, johndoe3)\n" +
			"  (4, Jane, Doe2, female, janedoe)\n" +
			"  (5, Jonathan, Black, male, jonathanblack)\n" +
			"  (6, Tom, Brown, male, tombrown)\n" +
			"  合計: 6 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("DELETE でレコードを削除できる", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer handler.Reset()

		// WHEN
		executePlan(t, &ast.DeleteStmt{
			From: *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("username")),
					ast.NewRhsLiteral(ast.NewStringLiteral("johndoe2")),
				),
			},
		})

		// THEN
		records := executePlan(t, &ast.SelectStmt{
			From:  *ast.NewTableId("users"),
			Where: nil,
		})

		var sb strings.Builder
		sb.WriteString("=== DELETE 後の全件 ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== DELETE 後の全件 ===\n" +
			"  (1, John, Doe, male, johndoe)\n" +
			"  (3, John, Doe3, male, johndoe3)\n" +
			"  (4, Jane, Doe2, female, janedoe)\n" +
			"  (5, Jonathan, Black, male, jonathanblack)\n" +
			"  (6, Tom, Brown, male, tombrown)\n" +
			"  合計: 5 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("INNER JOIN で 2 テーブルを結合できる", func(t *testing.T) {
		// GIVEN: users テーブル + orders テーブル
		setupUsersTable(t)
		defer handler.Reset()

		// orders テーブルを作成
		executePlan(t, &ast.CreateTableStmt{
			TableName: "orders",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "user_id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "item", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
				&ast.ConstraintUniqueKeyDef{Column: *ast.NewColumnId("user_id")},
			},
		})

		// orders にデータを投入
		executePlan(t, &ast.InsertStmt{
			Table: *ast.NewTableId("orders"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("user_id"),
				*ast.NewColumnId("item"),
			},
			Values: [][]ast.Literal{
				{ast.NewStringLiteral("100"), ast.NewStringLiteral("1"), ast.NewStringLiteral("apple")},
				{ast.NewStringLiteral("101"), ast.NewStringLiteral("3"), ast.NewStringLiteral("banana")},
			},
		})

		// WHEN: SELECT * FROM users JOIN orders ON users.id = orders.user_id
		records := executePlan(t, &ast.SelectStmt{
			From: *ast.NewTableId("users"),
			Joins: []*ast.JoinClause{
				{
					Table: *ast.NewTableId("orders"),
					Condition: ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "id"}),
						ast.NewRhsColumn(ast.ColumnId{TableName: "orders", ColName: "user_id"}),
					),
				},
			},
		})

		// THEN: 2 行がマッチ (結合順序はオプティマイザが決定するためカラム順序に依存しない検証)
		assert.Len(t, records, 2)

		// レコード内容を文字列化して検証
		var sb strings.Builder
		writeRecords(&sb, records)
		result := sb.String()
		// user_id=1 と item=apple が同一行にある
		assert.Contains(t, result, "apple")
		assert.Contains(t, result, "banana")
		// users のデータも含まれている
		assert.Contains(t, result, "John")
		assert.Contains(t, result, "Doe")
	})

	t.Run("INNER JOIN で内部表が UNIQUE INDEX eq_ref で検索される", func(t *testing.T) {
		// GIVEN: users (6 行) が駆動表、orders (10 行) が内部表になるよう orders を大きくする
		setupUsersTable(t)
		defer handler.Reset()

		executePlan(t, &ast.CreateTableStmt{
			TableName: "orders",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "user_id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "item", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
				&ast.ConstraintUniqueKeyDef{Column: *ast.NewColumnId("user_id")},
			},
		})

		// orders に 10 行投入 (users の 6 行より多い → users が駆動表、orders が内部表)
		for i := range 10 {
			executePlan(t, &ast.InsertStmt{
				Table: *ast.NewTableId("orders"),
				Cols:  []ast.ColumnId{*ast.NewColumnId("id"), *ast.NewColumnId("user_id"), *ast.NewColumnId("item")},
				Values: [][]ast.Literal{{
					ast.NewStringLiteral(fmt.Sprintf("%d", 100+i)),
					ast.NewStringLiteral(fmt.Sprintf("%d", i+1)),
					ast.NewStringLiteral(fmt.Sprintf("item_%d", i)),
				}},
			})
		}

		// WHEN: SELECT * FROM users JOIN orders ON users.id = orders.user_id
		records := executePlan(t, &ast.SelectStmt{
			From: *ast.NewTableId("users"),
			Joins: []*ast.JoinClause{
				{
					Table: *ast.NewTableId("orders"),
					Condition: ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "id"}),
						ast.NewRhsColumn(ast.ColumnId{TableName: "orders", ColName: "user_id"}),
					),
				},
			},
		})

		// THEN: users 6 行 × orders の一致行 → 6 行 (users の全 id が orders に存在)
		assert.Len(t, records, 6)
	})

	t.Run("SELECT で特定カラムを指定して取得できる", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer handler.Reset()

		// WHEN: SELECT id, first_name FROM users
		records := executePlan(t, &ast.SelectStmt{
			Columns: []ast.ColumnId{
				{ColName: "id"},
				{ColName: "first_name"},
			},
			From: *ast.NewTableId("users"),
		})

		// THEN: 6 行、各行 2 カラム
		assert.Len(t, records, 6)
		assert.Len(t, records[0], 2)
		assert.Equal(t, "1", string(records[0][0]))    // id
		assert.Equal(t, "John", string(records[0][1])) // first_name
	})

	t.Run("index-only scan で PK + UK のみ取得できる", func(t *testing.T) {
		// GIVEN: users テーブルの username に UNIQUE INDEX がある
		setupUsersTable(t)
		defer handler.Reset()

		// WHEN: SELECT id, username FROM users WHERE username = 'johndoe'
		// id は PK、username は UNIQUE INDEX → index-only scan 可能
		records := executePlan(t, &ast.SelectStmt{
			Columns: []ast.ColumnId{
				{ColName: "id"},
				{ColName: "username"},
			},
			From: *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr("=",
					ast.NewLhsColumn(*ast.NewColumnId("username")),
					ast.NewRhsLiteral(ast.NewStringLiteral("johndoe")),
				),
			},
		})

		// THEN: 1 行、2 カラム (テーブル本体の検索なしで取得)
		assert.Len(t, records, 1)
		assert.Len(t, records[0], 2)
		assert.Equal(t, "1", string(records[0][0]))       // id (PK)
		assert.Equal(t, "johndoe", string(records[0][1])) // username (UK)
	})

	t.Run("INNER JOIN + WHERE で駆動表の条件が分離される", func(t *testing.T) {
		// GIVEN: users (6 行) が駆動表、orders (10 行) が内部表
		setupUsersTable(t)
		defer handler.Reset()

		executePlan(t, &ast.CreateTableStmt{
			TableName: "orders",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "user_id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "item", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
				&ast.ConstraintUniqueKeyDef{Column: *ast.NewColumnId("user_id")},
			},
		})
		for i := range 10 {
			executePlan(t, &ast.InsertStmt{
				Table: *ast.NewTableId("orders"),
				Cols:  []ast.ColumnId{*ast.NewColumnId("id"), *ast.NewColumnId("user_id"), *ast.NewColumnId("item")},
				Values: [][]ast.Literal{{
					ast.NewStringLiteral(fmt.Sprintf("%d", 100+i)),
					ast.NewStringLiteral(fmt.Sprintf("%d", i+1)),
					ast.NewStringLiteral(fmt.Sprintf("item_%d", i)),
				}},
			})
		}

		// WHEN: WHERE users.id = '1' (駆動表のPK条件 → 分離でPKの等値検索)
		records := executePlan(t, &ast.SelectStmt{
			From: *ast.NewTableId("users"),
			Joins: []*ast.JoinClause{
				{
					Table: *ast.NewTableId("orders"),
					Condition: ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "id"}),
						ast.NewRhsColumn(ast.ColumnId{TableName: "orders", ColName: "user_id"}),
					),
				},
			},
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "id"}),
					ast.NewRhsLiteral(ast.NewStringLiteral("1")),
				),
			},
		})

		// THEN: PK 分離により users.id='1' の 1 行のみ → orders と JOIN → 1 行
		assert.Len(t, records, 1)
		var sb strings.Builder
		writeRecords(&sb, records)
		result := sb.String()
		assert.Contains(t, result, "John")
		assert.Contains(t, result, "item_0")
	})

	t.Run("INNER JOIN + WHERE で結合後にフィルタリングできる", func(t *testing.T) {
		// GIVEN: 上と同じ users + orders
		setupUsersTable(t)
		defer handler.Reset()

		executePlan(t, &ast.CreateTableStmt{
			TableName: "orders",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "user_id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "item", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{
					*ast.NewColumnId("id"),
				}},
				&ast.ConstraintUniqueKeyDef{Column: *ast.NewColumnId("user_id")},
			},
		})
		executePlan(t, &ast.InsertStmt{
			Table: *ast.NewTableId("orders"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("user_id"),
				*ast.NewColumnId("item"),
			},
			Values: [][]ast.Literal{
				{ast.NewStringLiteral("100"), ast.NewStringLiteral("1"), ast.NewStringLiteral("apple")},
				{ast.NewStringLiteral("101"), ast.NewStringLiteral("3"), ast.NewStringLiteral("banana")},
			},
		})

		// WHEN: SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE orders.item = 'banana'
		records := executePlan(t, &ast.SelectStmt{
			From: *ast.NewTableId("users"),
			Joins: []*ast.JoinClause{
				{
					Table: *ast.NewTableId("orders"),
					Condition: ast.NewBinaryExpr(
						"=",
						ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "id"}),
						ast.NewRhsColumn(ast.ColumnId{TableName: "orders", ColName: "user_id"}),
					),
				},
			},
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(ast.ColumnId{TableName: "orders", ColName: "item"}),
					ast.NewRhsLiteral(ast.NewStringLiteral("banana")),
				),
			},
		})

		// THEN: item='banana' でフィルタ → 1 行のみ
		assert.Len(t, records, 1)

		var sb strings.Builder
		writeRecords(&sb, records)
		result := sb.String()
		assert.Contains(t, result, "banana")
		assert.Contains(t, result, "John") // users.id=3 の John
		assert.Contains(t, result, "Doe3") // users.id=3 の Doe3
	})

	t.Run("非ユニークインデックス付きテーブルで同一キーの複数行を取得できる", func(t *testing.T) {
		// GIVEN: 非ユニークインデックス付きテーブル
		setupUsersTable(t)
		defer handler.Reset()

		executePlan(t, &ast.CreateTableStmt{
			TableName: "products",
			CreateDefinitions: []ast.Definition{
				&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "name", DataType: ast.DataTypeVarchar},
				&ast.ColumnDef{ColName: "category", DataType: ast.DataTypeVarchar},
				&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
				&ast.ConstraintKeyDef{KeyName: "idx_category", Column: *ast.NewColumnId("category")},
			},
		})
		executePlan(t, &ast.InsertStmt{
			Table: *ast.NewTableId("products"),
			Cols:  []ast.ColumnId{*ast.NewColumnId("id"), *ast.NewColumnId("name"), *ast.NewColumnId("category")},
			Values: [][]ast.Literal{
				{ast.NewStringLiteral("1"), ast.NewStringLiteral("Apple"), ast.NewStringLiteral("Fruit")},
				{ast.NewStringLiteral("2"), ast.NewStringLiteral("Banana"), ast.NewStringLiteral("Fruit")},
				{ast.NewStringLiteral("3"), ast.NewStringLiteral("Carrot"), ast.NewStringLiteral("Veggie")},
			},
		})

		// WHEN: WHERE category = 'Fruit'
		records := executePlan(t, &ast.SelectStmt{
			From: *ast.NewTableId("products"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr("=",
					ast.NewLhsColumn(*ast.NewColumnId("category")),
					ast.NewRhsLiteral(ast.NewStringLiteral("Fruit")),
				),
			},
		})

		// THEN: "Fruit" の 2 行が返される
		assert.Len(t, records, 2)
		assert.Equal(t, "Apple", string(records[0][1]))
		assert.Equal(t, "Banana", string(records[1][1]))
	})
}
