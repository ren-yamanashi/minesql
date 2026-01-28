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

func NewBinaryExpr(left identifier.ColumnId, right literal.Literal) *BinaryExpr {
	return &BinaryExpr{
		ExprType: ExprTypeBinary,
		Left:     left,
		Right:    right,
	}
}

func (be *BinaryExpr) expressionNode() {}
