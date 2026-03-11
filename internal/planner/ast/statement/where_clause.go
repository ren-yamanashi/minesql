package statement

import "minesql/internal/planner/ast/expression"

type WhereClause struct {
	Condition expression.Expression
	IsSet     bool
}

func NewWhereClause(condition expression.Expression) *WhereClause {
	return &WhereClause{
		Condition: condition,
		IsSet:     true,
	}
}
