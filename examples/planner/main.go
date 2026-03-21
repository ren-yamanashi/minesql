package main

import (
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/engine"
	"minesql/internal/executor"
	"minesql/internal/planner"
	"os"
)

// Executor から全レコードを取得する
func executePlan(exec executor.Executor) ([]executor.Record, error) {
	var records []executor.Record
	for {
		record, err := exec.Next()
		if err != nil {
			return nil, err
		}
		if record == nil {
			return records, nil
		}
		records = append(records, record)
	}
}

func main() {
	dataDir := "examples/planner/data"
	os.RemoveAll(dataDir) // 既存のデータディレクトリがあれば削除
	os.MkdirAll(dataDir, 0750)

	// StorageManager を初期化
	os.Setenv("MINESQL_DATA_DIR", dataDir)
	os.Setenv("MINESQL_BUFFER_SIZE", "100")
	engine.Init()

	createTable()
	insert()
	scan()
	assertEqual()
	filter()
	updateByCondition()
	scanAfterUpdate()
	deleteByCondition()
	scanAfterDelete()
}

func createTable() {
	stmt := &ast.CreateTableStmt{
		StmtType: ast.StmtTypeCreate,
		Keyword:  ast.KeywordTable,
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
	}

	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	records, err := executePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		println(string(record[0]), string(record[1]), string(record[2]), string(record[3]), string(record[4]))
	}
}

func insert() {
	stmt := &ast.InsertStmt{
		StmtType: ast.StmtTypeInsert,
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
	}

	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	records, err := executePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		println(string(record[0]), string(record[1]), string(record[2]), string(record[3]), string(record[4]))
	}
}

func scan() {
	fmt.Println("=== scan all ===")
	stmt := &ast.SelectStmt{
		StmtType: ast.StmtTypeSelect,
		From:     *ast.NewTableId("users"),
		Where:    &ast.WhereClause{IsSet: false},
	}
	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	records, err := executePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		for _, col := range record {
			fmt.Print(string(col), " ")
		}
		fmt.Println()
	}
}

func assertEqual() {
	fmt.Println("=== assert equal ===")
	stmt := &ast.SelectStmt{
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
	}
	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	records, err := executePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		for _, col := range record {
			fmt.Print(string(col), " ")
		}
		fmt.Println()
	}
}

func updateByCondition() {
	fmt.Println("=== UPDATE users SET last_name = 'Smith' WHERE username = 'johndoe' ===")
	// UPDATE users SET last_name = 'Smith' WHERE username = 'johndoe'
	stmt := &ast.UpdateStmt{
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
	}
	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	_, err = executePlan(exec)
	if err != nil {
		panic(err)
	}
	fmt.Println("updated.")
}

func scanAfterUpdate() {
	fmt.Println("=== scan after update ===")
	stmt := &ast.SelectStmt{
		StmtType: ast.StmtTypeSelect,
		From:     *ast.NewTableId("users"),
		Where:    &ast.WhereClause{IsSet: false},
	}
	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	records, err := executePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		for _, col := range record {
			fmt.Print(string(col), " ")
		}
		fmt.Println()
	}
}

func filter() {
	fmt.Println("=== filter (first_name < 'K' AND gender = 'male' AND last_name >= 'Doe') OR first_name = 'Tom' ===")
	// SELECT * FROM users WHERE (first_name < 'K' AND gender = 'male' AND last_name >= 'Doe') OR first_name = 'Tom'
	stmt := &ast.SelectStmt{
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
	}
	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	records, err := executePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		for _, col := range record {
			fmt.Print(string(col), " ")
		}
		fmt.Println()
	}
}

func deleteByCondition() {
	fmt.Println("=== delete WHERE username = 'johndoe2' ===")
	// DELETE FROM users WHERE username = 'johndoe2'
	stmt := &ast.DeleteStmt{
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
	}
	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	_, err = executePlan(exec)
	if err != nil {
		panic(err)
	}
	fmt.Println("deleted.")
}

func scanAfterDelete() {
	fmt.Println("=== scan after delete ===")
	stmt := &ast.SelectStmt{
		StmtType: ast.StmtTypeSelect,
		From:     *ast.NewTableId("users"),
		Where:    &ast.WhereClause{IsSet: false},
	}
	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	records, err := executePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		for _, col := range record {
			fmt.Print(string(col), " ")
		}
		fmt.Println()
	}
}
