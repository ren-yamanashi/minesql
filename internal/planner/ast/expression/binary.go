package expression

import (
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/literal"
)

type BinaryExpr struct {
	ExprType ExprType
	Operator string
	Left     LHS
	Right    RHS
}

func NewBinaryExpr(operator string, left LHS, right RHS) *BinaryExpr {
	return &BinaryExpr{
		ExprType: ExprTypeBinary,
		Operator: operator,
		Left:     left,
		Right:    right,
	}
}

func (be *BinaryExpr) expressionNode() {}

// ===========================
// LHS
// ===========================
type LHS interface {
	lhsNode()
}

type LhsColumn struct {
	Column identifier.ColumnId
}

func NewLhsColumn(col identifier.ColumnId) *LhsColumn {
	return &LhsColumn{
		Column: col,
	}
}

func (lc *LhsColumn) lhsNode() {}

type LhsExpr struct {
	Expr Expression
}

func NewLhsExpr(expr Expression) *LhsExpr {
	return &LhsExpr{
		Expr: expr,
	}
}

func (le *LhsExpr) lhsNode() {}

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
