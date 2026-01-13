package planner

import (
	"minesql/internal/executor"
	"minesql/internal/planner"
	"minesql/internal/planner/ast/definition"
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/statement"
)

func createTable() {
	ast := statement.NewCreateTableStmt(
		"users",
		[]definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeInt),
			definition.NewColumnDef("first_name", definition.DataTypeVarchar),
			definition.NewColumnDef("last_name", definition.DataTypeVarchar),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("id"),
			}),
			definition.NewConstraintUniqueKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("first_name"),
			}),
			definition.NewConstraintUniqueKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("last_name"),
			}),
		},
	)

	plan := planner.NewPlanner()
	exec, err := plan.PlanStart(ast)
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
