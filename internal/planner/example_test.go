package planner_test

import (
	"fmt"
	"os"
	"strings"

	"minesql/internal/ast"
	"minesql/internal/engine"
	"minesql/internal/executor"
	"minesql/internal/planner"
)

// セットアップヘルパー: テーブルを作成し、サンプルデータを挿入する
func setupPlannerExample() func() {
	tmpDir, err := os.MkdirTemp("", "planner_example")
	if err != nil {
		panic(err)
	}
	cleanup := func() {
		engine.Reset()
		_ = os.RemoveAll(tmpDir)
	}

	if err = os.Setenv("MINESQL_DATA_DIR", tmpDir); err != nil {
		panic(err)
	}
	if err = os.Setenv("MINESQL_BUFFER_SIZE", "100"); err != nil {
		panic(err)
	}
	engine.Reset()
	engine.Init()

	// CREATE TABLE
	trx := executor.Begin(0)
	runPlan(trx, &ast.CreateTableStmt{
		StmtType:  ast.StmtTypeCreate,
		Keyword:   ast.KeywordTable,
		TableName: "users",
		CreateDefinitions: []ast.Definition{
			&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "first_name", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "last_name", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "gender", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "username", DataType: ast.DataTypeVarchar},
			&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{
				*ast.NewColumnId("id"),
			}},
			&ast.ConstraintUniqueKeyDef{DefType: ast.DefTypeConstraintUniqueKey, Column: *ast.NewColumnId("username")},
		},
	})

	// INSERT
	runPlan(trx, &ast.InsertStmt{
		StmtType: ast.StmtTypeInsert,
		Table:    *ast.NewTableId("users"),
		Cols: []ast.ColumnId{
			*ast.NewColumnId("id"),
			*ast.NewColumnId("first_name"),
			*ast.NewColumnId("last_name"),
			*ast.NewColumnId("gender"),
			*ast.NewColumnId("username"),
		},
		Values: [][]ast.Literal{
			{
				ast.NewStringLiteral("1", "1"),
				ast.NewStringLiteral("John", "John"),
				ast.NewStringLiteral("Doe", "Doe"),
				ast.NewStringLiteral("male", "male"),
				ast.NewStringLiteral("johndoe", "johndoe"),
			},
			{
				ast.NewStringLiteral("2", "2"),
				ast.NewStringLiteral("John", "John"),
				ast.NewStringLiteral("Doe2", "Doe2"),
				ast.NewStringLiteral("male", "male"),
				ast.NewStringLiteral("johndoe2", "johndoe2"),
			},
			{
				ast.NewStringLiteral("3", "3"),
				ast.NewStringLiteral("John", "John"),
				ast.NewStringLiteral("Doe3", "Doe3"),
				ast.NewStringLiteral("male", "male"),
				ast.NewStringLiteral("johndoe3", "johndoe3"),
			},
			{
				ast.NewStringLiteral("4", "4"),
				ast.NewStringLiteral("Jane", "Jane"),
				ast.NewStringLiteral("Doe2", "Doe2"),
				ast.NewStringLiteral("female", "female"),
				ast.NewStringLiteral("janedoe", "janedoe"),
			},
			{
				ast.NewStringLiteral("5", "5"),
				ast.NewStringLiteral("Jonathan", "Jonathan"),
				ast.NewStringLiteral("Black", "Black"),
				ast.NewStringLiteral("male", "male"),
				ast.NewStringLiteral("jonathanblack", "jonathanblack"),
			},
			{
				ast.NewStringLiteral("6", "6"),
				ast.NewStringLiteral("Tom", "Tom"),
				ast.NewStringLiteral("Brown", "Brown"),
				ast.NewStringLiteral("male", "male"),
				ast.NewStringLiteral("tombrown", "tombrown"),
			},
		},
	})
	trx.Commit()

	return cleanup
}

// AST を直接構築 → planner.Start → 実行して結果を返す
func runPlan(trx *executor.Transaction, stmt ast.Statement) []executor.Record {
	exec, err := planner.Start(trx, stmt)
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

	trx := executor.Begin(0)
	records := runPlan(trx, &ast.SelectStmt{
		StmtType: ast.StmtTypeSelect,
		From:     *ast.NewTableId("users"),
		Where:    &ast.WhereClause{IsSet: false},
	})
	trx.Commit()
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

	trx := executor.Begin(0)
	records := runPlan(trx, &ast.SelectStmt{
		StmtType: ast.StmtTypeSelect,
		From:     *ast.NewTableId("users"),
		Where: &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("username")),
				ast.NewRhsLiteral(ast.NewStringLiteral("janedoe", "janedoe")),
			),
			IsSet: true,
		},
	})
	trx.Commit()
	printPlanRecords(records)

	// Output:
	//   (4, Jane, Doe2, female, janedoe)
	//   合計: 1 件
}

func Example_filter() {
	cleanup := setupPlannerExample()
	defer cleanup()

	// SELECT * FROM users WHERE (first_name < 'K' AND gender = 'male' AND last_name >= 'Doe') OR first_name = 'Tom'
	trx := executor.Begin(0)
	records := runPlan(trx, &ast.SelectStmt{
		StmtType: ast.StmtTypeSelect,
		From:     *ast.NewTableId("users"),
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
								ast.NewRhsLiteral(ast.NewStringLiteral("K", "K")),
							),
						),
						ast.NewRhsExpr(
							ast.NewBinaryExpr(
								"AND",
								ast.NewLhsExpr(
									ast.NewBinaryExpr(
										"=",
										ast.NewLhsColumn(*ast.NewColumnId("gender")),
										ast.NewRhsLiteral(ast.NewStringLiteral("male", "male")),
									),
								),
								ast.NewRhsExpr(
									ast.NewBinaryExpr(
										">=",
										ast.NewLhsColumn(*ast.NewColumnId("last_name")),
										ast.NewRhsLiteral(ast.NewStringLiteral("Doe", "Doe")),
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
						ast.NewRhsLiteral(ast.NewStringLiteral("Tom", "Tom")),
					),
				),
			),
			IsSet: true,
		},
	})
	trx.Commit()
	printPlanRecords(records)

	// Output:
	//   (1, John, Doe, male, johndoe)
	//   (2, John, Doe2, male, johndoe2)
	//   (3, John, Doe3, male, johndoe3)
	//   (6, Tom, Brown, male, tombrown)
	//   合計: 4 件
}

func Example_update() {
	cleanup := setupPlannerExample()
	defer cleanup()

	trx := executor.Begin(0)
	// UPDATE users SET last_name = 'Smith' WHERE username = 'johndoe'
	runPlan(trx, &ast.UpdateStmt{
		Table: *ast.NewTableId("users"),
		SetClauses: []*ast.SetClause{
			{Column: *ast.NewColumnId("last_name"), Value: ast.NewStringLiteral("'Smith'", "Smith")},
		},
		Where: &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("username")),
				ast.NewRhsLiteral(ast.NewStringLiteral("johndoe", "johndoe")),
			),
			IsSet: true,
		},
	})

	records := runPlan(trx, &ast.SelectStmt{
		StmtType: ast.StmtTypeSelect,
		From:     *ast.NewTableId("users"),
		Where:    &ast.WhereClause{IsSet: false},
	})
	trx.Commit()
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

	trx := executor.Begin(0)
	// DELETE FROM users WHERE username = 'johndoe2'
	runPlan(trx, &ast.DeleteStmt{
		StmtType: ast.StmtTypeDelete,
		From:     *ast.NewTableId("users"),
		Where: &ast.WhereClause{
			Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("username")),
				ast.NewRhsLiteral(ast.NewStringLiteral("johndoe2", "johndoe2")),
			),
			IsSet: true,
		},
	})

	records := runPlan(trx, &ast.SelectStmt{
		StmtType: ast.StmtTypeSelect,
		From:     *ast.NewTableId("users"),
		Where:    &ast.WhereClause{IsSet: false},
	})
	trx.Commit()
	printPlanRecords(records)

	// Output:
	//   (1, John, Doe, male, johndoe)
	//   (3, John, Doe3, male, johndoe3)
	//   (4, Jane, Doe2, female, janedoe)
	//   (5, Jonathan, Black, male, jonathanblack)
	//   (6, Tom, Brown, male, tombrown)
	//   合計: 5 件
}
