package statement

import (
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/identifier"
)

type WhereClause struct {
	Condition expression.Expression
	IsSet     bool
}

type SelectStmt struct {
	StmtType StmtType
	Columns  []identifier.ColumnId
	From     identifier.TableId
	Where    WhereClause
}
