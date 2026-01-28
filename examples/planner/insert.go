package planner

import (
	"minesql/internal/executor"
	"minesql/internal/planner"
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/literal"
	"minesql/internal/planner/ast/statement"
)

func insert() {
	stmt := statement.NewInsertStmt(
		*identifier.NewTableId("users", "sample"),
		[]identifier.ColumnId{
			*identifier.NewColumnId("id"),
			*identifier.NewColumnId("first_name"),
			*identifier.NewColumnId("last_name"),
		},
		[][]literal.Literal{
			{
				literal.NewStringLiteral("1", "1"),
				literal.NewStringLiteral("John", "John"),
				literal.NewStringLiteral("Doe", "Doe"),
			},
			{
				literal.NewStringLiteral("2", "2"),
				literal.NewStringLiteral("Jane", "Jane"),
				literal.NewStringLiteral("Smith", "Smith"),
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
