package main

import (
	"fmt"
	"minesql/internal/executor"
	"minesql/internal/planner"
	"minesql/internal/planner/ast/definition"
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/literal"
	"minesql/internal/planner/ast/statement"
	"minesql/internal/storage"
	"os"
)

func main() {
	dataDir := "examples/planner/data"
	os.RemoveAll(dataDir) // 既存のデータディレクトリがあれば削除
	os.MkdirAll(dataDir, 0750)

	// StorageManager を初期化
	os.Setenv("MINESQL_DATA_DIR", dataDir)
	os.Setenv("MINESQL_BUFFER_SIZE", "100")
	storage.InitStorageManager()

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
	stmt := statement.NewCreateTableStmt(
		"users",
		[]definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeVarchar),
			definition.NewColumnDef("first_name", definition.DataTypeVarchar),
			definition.NewColumnDef("last_name", definition.DataTypeVarchar),
			definition.NewColumnDef("gender", definition.DataTypeVarchar),
			definition.NewColumnDef("username", definition.DataTypeVarchar),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("id"),
			}),
			definition.NewConstraintUniqueKeyDef(*identifier.NewColumnId("username")),
		},
	)

	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	records, err := executor.ExecutePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		println(string(record[0]), string(record[1]), string(record[2]), string(record[3]), string(record[4]))
	}
}

func insert() {
	stmt := statement.NewInsertStmt(
		*identifier.NewTableId("users"),
		[]identifier.ColumnId{
			*identifier.NewColumnId("id"),
			*identifier.NewColumnId("first_name"),
			*identifier.NewColumnId("last_name"),
			*identifier.NewColumnId("gender"),
			*identifier.NewColumnId("username"),
		},
		[][]literal.Literal{
			{
				literal.NewStringLiteral("1", "1"),
				literal.NewStringLiteral("John", "John"),
				literal.NewStringLiteral("Doe", "Doe"),
				literal.NewStringLiteral("male", "male"),
				literal.NewStringLiteral("johndoe", "johndoe"),
			},
			{
				literal.NewStringLiteral("2", "2"),
				literal.NewStringLiteral("John", "John"),
				literal.NewStringLiteral("Doe2", "Doe2"),
				literal.NewStringLiteral("male", "male"),
				literal.NewStringLiteral("johndoe2", "johndoe2"),
			},
			{
				literal.NewStringLiteral("3", "3"),
				literal.NewStringLiteral("John", "John"),
				literal.NewStringLiteral("Doe3", "Doe3"),
				literal.NewStringLiteral("male", "male"),
				literal.NewStringLiteral("johndoe3", "johndoe3"),
			},
			{
				literal.NewStringLiteral("4", "4"),
				literal.NewStringLiteral("Jane", "Jane"),
				literal.NewStringLiteral("Doe2", "Doe2"),
				literal.NewStringLiteral("female", "female"),
				literal.NewStringLiteral("janedoe", "janedoe"),
			},
			{
				literal.NewStringLiteral("5", "5"),
				literal.NewStringLiteral("Jonathan", "Jonathan"),
				literal.NewStringLiteral("Black", "Black"),
				literal.NewStringLiteral("male", "male"),
				literal.NewStringLiteral("jonathanblack", "jonathanblack"),
			},
			{
				literal.NewStringLiteral("6", "6"),
				literal.NewStringLiteral("Tom", "Tom"),
				literal.NewStringLiteral("Brown", "Brown"),
				literal.NewStringLiteral("male", "male"),
				literal.NewStringLiteral("tombrown", "tombrown"),
			},
		},
	)

	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	records, err := executor.ExecutePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		println(string(record[0]), string(record[1]), string(record[2]), string(record[3]), string(record[4]))
	}
}

func scan() {
	fmt.Println("=== scan all ===")
	stmt := statement.NewSelectStmt(
		*identifier.NewTableId("users"),
		nil,
	)
	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	records, err := executor.ExecutePlan(exec)
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
	stmt := statement.NewSelectStmt(
		*identifier.NewTableId("users"),
		statement.NewWhereClause(
			expression.NewBinaryExpr(
				"=",
				expression.NewLhsColumn(*identifier.NewColumnId("username")),
				expression.NewRhsLiteral(literal.NewStringLiteral("janedoe", "janedoe")),
			),
		),
	)
	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	records, err := executor.ExecutePlan(exec)
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
	stmt := &statement.UpdateStmt{
		Table: *identifier.NewTableId("users"),
		SetClauses: []*statement.SetClause{
			{Column: *identifier.NewColumnId("last_name"), Value: literal.NewStringLiteral("'Smith'", "Smith")},
		},
		Where: statement.NewWhereClause(
			expression.NewBinaryExpr(
				"=",
				expression.NewLhsColumn(*identifier.NewColumnId("username")),
				expression.NewRhsLiteral(literal.NewStringLiteral("johndoe", "johndoe")),
			),
		),
	}
	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	_, err = executor.ExecutePlan(exec)
	if err != nil {
		panic(err)
	}
	fmt.Println("updated.")
}

func scanAfterUpdate() {
	fmt.Println("=== scan after update ===")
	stmt := statement.NewSelectStmt(
		*identifier.NewTableId("users"),
		nil,
	)
	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	records, err := executor.ExecutePlan(exec)
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
	stmt := statement.NewDeleteStmt(
		*identifier.NewTableId("users"),
		statement.NewWhereClause(
			expression.NewBinaryExpr(
				"=",
				expression.NewLhsColumn(*identifier.NewColumnId("username")),
				expression.NewRhsLiteral(literal.NewStringLiteral("johndoe2", "johndoe2")),
			),
		),
	)
	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	_, err = executor.ExecutePlan(exec)
	if err != nil {
		panic(err)
	}
	fmt.Println("deleted.")
}

func scanAfterDelete() {
	fmt.Println("=== scan after delete ===")
	stmt := statement.NewSelectStmt(
		*identifier.NewTableId("users"),
		nil,
	)
	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	records, err := executor.ExecutePlan(exec)
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
	fmt.Println("=== filter ===")
	stmt := statement.NewSelectStmt(
		*identifier.NewTableId("users"),
		statement.NewWhereClause(
			expression.NewBinaryExpr(
				"AND",
				expression.NewLhsExpr(
					expression.NewBinaryExpr(
						"<",
						expression.NewLhsColumn(*identifier.NewColumnId("first_name")),
						expression.NewRhsLiteral(literal.NewStringLiteral("K", "K")),
					),
				),
				expression.NewRhsExpr(
					expression.NewBinaryExpr(
						"AND",
						expression.NewLhsExpr(
							expression.NewBinaryExpr(
								"=",
								expression.NewLhsColumn(*identifier.NewColumnId("gender")),
								expression.NewRhsLiteral(literal.NewStringLiteral("male", "male")),
							),
						),
						expression.NewRhsExpr(
							expression.NewBinaryExpr(
								">=",
								expression.NewLhsColumn(*identifier.NewColumnId("last_name")),
								expression.NewRhsLiteral(literal.NewStringLiteral("Doe", "Doe")),
							),
						),
					),
				),
			),
		),
	)
	exec, err := planner.PlanStart(stmt)
	if err != nil {
		panic(err)
	}
	records, err := executor.ExecutePlan(exec)
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
