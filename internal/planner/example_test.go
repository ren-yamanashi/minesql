package planner_test

import (
	"fmt"
	"os"
	"strings"

	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/planner"
	"minesql/internal/storage/handler"
)

// セットアップヘルパー: テーブルを作成し、サンプルデータを挿入する
func setupPlannerExample() func() {
	tmpDir, err := os.MkdirTemp("", "planner_example")
	if err != nil {
		panic(err)
	}
	cleanup := func() {
		handler.Reset()
		_ = os.RemoveAll(tmpDir)
	}

	if err = os.Setenv("MINESQL_DATA_DIR", tmpDir); err != nil {
		panic(err)
	}
	if err = os.Setenv("MINESQL_BUFFER_SIZE", "100"); err != nil {
		panic(err)
	}
	handler.Reset()
	handler.Init()

	// CREATE TABLE
	runPlan(&ast.CreateTableStmt{
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
	runPlan(&ast.InsertStmt{
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

	return cleanup
}

// AST を直接構築 → planner.Start → 実行して結果を返す
func runPlan(stmt ast.Statement) []executor.Record {
	var trxId handler.TrxId = 1
	exec, err := planner.Start(trxId, stmt)
	if err != nil {
		panic(err)
	}

	var records []executor.Record
	for {
		record, err := exec.Next()
		if err != nil {
			panic(err)
		}
		if record == nil {
			return records
		}
		records = append(records, record)
	}
}

// レコードを表示するヘルパー
func printPlanRecords(records []executor.Record) {
	for _, record := range records {
		cols := make([]string, len(record))
		for i, col := range record {
			cols[i] = string(col)
		}
		fmt.Printf("  (%s)\n", strings.Join(cols, ", "))
	}
	fmt.Printf("  合計: %d 件\n", len(records))
}

func Example_scanAll() {
	cleanup := setupPlannerExample()
	defer cleanup()

	records := runPlan(&ast.SelectStmt{
		From:  *ast.NewTableId("users"),
		Where: nil,
	})
	printPlanRecords(records)

	// Output:
	//   (1, John, Doe, male, johndoe)
	//   (2, John, Doe2, male, johndoe2)
	//   (3, John, Doe3, male, johndoe3)
	//   (4, Jane, Doe2, female, janedoe)
	//   (5, Jonathan, Black, male, jonathanblack)
	//   (6, Tom, Brown, male, tombrown)
	//   合計: 6 件
}

func Example_assertEqual() {
	cleanup := setupPlannerExample()
	defer cleanup()

	records := runPlan(&ast.SelectStmt{
		From: *ast.NewTableId("users"),
		Where: &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("username")),
				ast.NewRhsLiteral(ast.NewStringLiteral("janedoe")),
			),
		},
	})
	printPlanRecords(records)

	// Output:
	//   (4, Jane, Doe2, female, janedoe)
	//   合計: 1 件
}

func Example_filter() {
	cleanup := setupPlannerExample()
	defer cleanup()

	// SELECT * FROM users WHERE (first_name < 'K' AND gender = 'male' AND last_name >= 'Doe') OR first_name = 'Tom'
	records := runPlan(&ast.SelectStmt{
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
	printPlanRecords(records)

	// Output:
	//   (1, John, Doe, male, johndoe)
	//   (2, John, Doe2, male, johndoe2)
	//   (3, John, Doe3, male, johndoe3)
	//   (6, Tom, Brown, male, tombrown)
	//   合計: 4 件
}

func Example_join() {
	cleanup := setupPlannerExample()
	defer cleanup()

	// CREATE TABLE orders
	runPlan(&ast.CreateTableStmt{
		TableName: "orders",
		CreateDefinitions: []ast.Definition{
			&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{ColName: "user_id", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{ColName: "item", DataType: ast.DataTypeVarchar},
			&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
			&ast.ConstraintUniqueKeyDef{KeyName: "idx_user_id", Column: *ast.NewColumnId("user_id")},
		},
	})
	runPlan(&ast.InsertStmt{
		Table: *ast.NewTableId("orders"),
		Cols:  []ast.ColumnId{*ast.NewColumnId("id"), *ast.NewColumnId("user_id"), *ast.NewColumnId("item")},
		Values: [][]ast.Literal{
			{ast.NewStringLiteral("100"), ast.NewStringLiteral("1"), ast.NewStringLiteral("apple")},
			{ast.NewStringLiteral("101"), ast.NewStringLiteral("3"), ast.NewStringLiteral("banana")},
		},
	})

	// SELECT * FROM users JOIN orders ON users.id = orders.user_id
	records := runPlan(&ast.SelectStmt{
		From: *ast.NewTableId("users"),
		Joins: []*ast.JoinClause{
			{
				Table: *ast.NewTableId("orders"),
				Condition: ast.NewBinaryExpr("=",
					ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "id"}),
					ast.NewRhsColumn(ast.ColumnId{TableName: "orders", ColName: "user_id"}),
				),
			},
		},
	})
	printPlanRecords(records)

	// Output:
	//   (100, 1, apple, 1, John, Doe, male, johndoe)
	//   (101, 3, banana, 3, John, Doe3, male, johndoe3)
	//   合計: 2 件
}

func Example_nonUniqueIndex() {
	cleanup := setupPlannerExample()
	defer cleanup()

	// 非ユニークインデックス付きテーブルを作成
	runPlan(&ast.CreateTableStmt{
		TableName: "products",
		CreateDefinitions: []ast.Definition{
			&ast.ColumnDef{ColName: "id", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{ColName: "name", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{ColName: "category", DataType: ast.DataTypeVarchar},
			&ast.ConstraintPrimaryKeyDef{Columns: []ast.ColumnId{*ast.NewColumnId("id")}},
			&ast.ConstraintKeyDef{KeyName: "idx_category", Column: *ast.NewColumnId("category")},
		},
	})
	runPlan(&ast.InsertStmt{
		Table: *ast.NewTableId("products"),
		Cols:  []ast.ColumnId{*ast.NewColumnId("id"), *ast.NewColumnId("name"), *ast.NewColumnId("category")},
		Values: [][]ast.Literal{
			{ast.NewStringLiteral("1"), ast.NewStringLiteral("Apple"), ast.NewStringLiteral("Fruit")},
			{ast.NewStringLiteral("2"), ast.NewStringLiteral("Banana"), ast.NewStringLiteral("Fruit")},
			{ast.NewStringLiteral("3"), ast.NewStringLiteral("Carrot"), ast.NewStringLiteral("Veggie")},
		},
	})

	// 同一カテゴリで複数行が取得できる
	records := runPlan(&ast.SelectStmt{
		From: *ast.NewTableId("products"),
		Where: &ast.WhereClause{
			Condition: ast.NewBinaryExpr("=",
				ast.NewLhsColumn(*ast.NewColumnId("category")),
				ast.NewRhsLiteral(ast.NewStringLiteral("Fruit")),
			),
		},
	})
	printPlanRecords(records)

	// Output:
	//   (1, Apple, Fruit)
	//   (2, Banana, Fruit)
	//   合計: 2 件
}

func Example_update() {
	cleanup := setupPlannerExample()
	defer cleanup()

	// UPDATE users SET last_name = 'Smith' WHERE username = 'johndoe'
	runPlan(&ast.UpdateStmt{
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

	records := runPlan(&ast.SelectStmt{
		From:  *ast.NewTableId("users"),
		Where: nil,
	})
	printPlanRecords(records)

	// Output:
	//   (1, John, Smith, male, johndoe)
	//   (2, John, Doe2, male, johndoe2)
	//   (3, John, Doe3, male, johndoe3)
	//   (4, Jane, Doe2, female, janedoe)
	//   (5, Jonathan, Black, male, jonathanblack)
	//   (6, Tom, Brown, male, tombrown)
	//   合計: 6 件
}

func Example_delete() {
	cleanup := setupPlannerExample()
	defer cleanup()

	// DELETE FROM users WHERE username = 'johndoe2'
	runPlan(&ast.DeleteStmt{
		From: *ast.NewTableId("users"),
		Where: &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("username")),
				ast.NewRhsLiteral(ast.NewStringLiteral("johndoe2")),
			),
		},
	})

	records := runPlan(&ast.SelectStmt{
		From:  *ast.NewTableId("users"),
		Where: nil,
	})
	printPlanRecords(records)

	// Output:
	//   (1, John, Doe, male, johndoe)
	//   (3, John, Doe3, male, johndoe3)
	//   (4, Jane, Doe2, female, janedoe)
	//   (5, Jonathan, Black, male, jonathanblack)
	//   (6, Tom, Brown, male, tombrown)
	//   合計: 5 件
}
