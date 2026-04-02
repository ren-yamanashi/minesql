package ast

type BinaryExpr struct {
	Operator string
	Left     LHS
	Right    RHS
}

func NewBinaryExpr(operator string, left LHS, right RHS) *BinaryExpr {
	return &BinaryExpr{
		Operator: operator,
		Left:     left,
		Right:    right,
	}
}

// -- LHS --

type LHS interface {
	isLHS()
}

type LhsColumn struct {
	Column ColumnId
}

func (*LhsColumn) isLHS() {}

func NewLhsColumn(col ColumnId) *LhsColumn {
	return &LhsColumn{
		Column: col,
	}
}

type LhsExpr struct {
	Expr *BinaryExpr
}

func (*LhsExpr) isLHS() {}

func NewLhsExpr(expr *BinaryExpr) *LhsExpr {
	return &LhsExpr{
		Expr: expr,
	}
}

// -- RHS --

type RHS interface {
	isRHS()
}

type RhsLiteral struct {
	Literal Literal
}

func (*RhsLiteral) isRHS() {}

func NewRhsLiteral(lit Literal) *RhsLiteral {
	return &RhsLiteral{
		Literal: lit,
	}
}

type RhsExpr struct {
	Expr *BinaryExpr
}

func (*RhsExpr) isRHS() {}

func NewRhsExpr(expr *BinaryExpr) *RhsExpr {
	return &RhsExpr{
		Expr: expr,
	}
}
