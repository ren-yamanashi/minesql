package ast

type ExprType string

const (
	ExprTypeBinary ExprType = "BINARY"
)

type Expression interface{}

// ===========================
// BinaryExpr
// ===========================

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

// -- LHS --

type LHS interface{}

type LhsColumn struct {
	Column ColumnId
}

func NewLhsColumn(col ColumnId) *LhsColumn {
	return &LhsColumn{
		Column: col,
	}
}

type LhsExpr struct {
	Expr Expression
}

func NewLhsExpr(expr Expression) *LhsExpr {
	return &LhsExpr{
		Expr: expr,
	}
}

// -- RHS --

type RHS interface{}

type RhsLiteral struct {
	Literal Literal
}

func NewRhsLiteral(lit Literal) *RhsLiteral {
	return &RhsLiteral{
		Literal: lit,
	}
}

type RhsExpr struct {
	Expr Expression
}

func NewRhsExpr(expr Expression) *RhsExpr {
	return &RhsExpr{
		Expr: expr,
	}
}
