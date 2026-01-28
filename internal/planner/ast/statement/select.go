package statement

import (
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/identifier"
)

type SelectStmt struct {
	StmtType StmtType
	Columns  []identifier.ColumnId
	From     identifier.TableId
	Where    expression.Expression
}
