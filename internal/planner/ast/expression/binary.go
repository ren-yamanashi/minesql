package expression

import (
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/literal"
)

type BinaryExpr struct {
	ExprType ExprType
	Left     identifier.ColumnId
	Right    literal.Literal
}

func (be *BinaryExpr) expressionNode() {}
