package expression

import (
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/literal"
)

type BinaryExpr struct {
	ExprType ExprType
	Operator string
	Left     identifier.ColumnId
	Right    RHS
}

func NewBinaryExpr(operator string, left identifier.ColumnId, right RHS) *BinaryExpr {
	return &BinaryExpr{
		ExprType: ExprTypeBinary,
		Operator: operator,
		Left:     left,
		Right:    right,
	}
}

func (be *BinaryExpr) expressionNode() {}

// ===========================
// RHS
// ===========================

type RHS interface {
	rhsNode()
}

type RhsLiteral struct {
	Literal literal.Literal
}

func NewRhsLiteral(lit literal.Literal) *RhsLiteral {
	return &RhsLiteral{
		Literal: lit,
	}
}

func (rl *RhsLiteral) rhsNode() {}

type RhsExpr struct {
	Expr Expression
}

func NewRhsExpr(expr Expression) *RhsExpr {
	return &RhsExpr{
		Expr: expr,
	}
}

func (re *RhsExpr) rhsNode() {}
