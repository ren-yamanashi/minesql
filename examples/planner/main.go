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
	os.MkdirAll(dataDir, 0755)

	// StorageManager を初期化
	os.Setenv("MINESQL_DATA_DIR", dataDir)
	os.Setenv("MINESQL_BUFFER_SIZE", "100")
	storage.InitStorageManager()

	createTable()
	insert()
	scan()
	assertEqual()
}

func createTable() {
	stmt := statement.NewCreateTableStmt(
		"users",
		[]definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeVarchar),
			definition.NewColumnDef("first_name", definition.DataTypeVarchar),
			definition.NewColumnDef("last_name", definition.DataTypeVarchar),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("id"),
			}),
			definition.NewConstraintUniqueKeyDef(*identifier.NewColumnId("last_name")),
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
		println(string(record[0]), string(record[1]), string(record[2]))
	}
}

func insert() {
	stmt := statement.NewInsertStmt(
		*identifier.NewTableId("users"),
		[]identifier.ColumnId{
			*identifier.NewColumnId("id"),
			*identifier.NewColumnId("last_name"),
			*identifier.NewColumnId("first_name"),
		},
		[][]literal.Literal{
			{
				literal.NewStringLiteral("1", "1"),
				literal.NewStringLiteral("Doe", "Doe"),
				literal.NewStringLiteral("John", "John"),
			},
			{
				literal.NewStringLiteral("2", "2"),
				literal.NewStringLiteral("Smith", "Smith"),
				literal.NewStringLiteral("Jane", "Jane"),
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
		println(string(record[0]), string(record[1]), string(record[2]))
	}
}

func scan() {
	stmt := statement.NewSelectStmt(
		*identifier.NewTableId("users"),
		statement.WhereClause{
			IsSet: false,
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
		for _, col := range record {
			fmt.Print(string(col), " ")
		}
		fmt.Println()
	}
}

func assertEqual() {
	stmt := statement.NewSelectStmt(
		*identifier.NewTableId("users"),
		statement.WhereClause{
			Condition: &expression.BinaryExpr{
				Left:  *identifier.NewColumnId("last_name"),
				Right: literal.NewStringLiteral("Smith", "Smith"),
			},
			IsSet: true,
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
		for _, col := range record {
			fmt.Print(string(col), " ")
		}
		fmt.Println()
	}
}
