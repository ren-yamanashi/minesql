package ast

type Statement interface {
	isStatement()
}

// ---------------------------------------
// Create Table
// ---------------------------------------

type CreateTableStmt struct {
	TableName         string
	CreateDefinitions []Definition
}

func (*CreateTableStmt) isStatement() {}

// ---------------------------------------
// Select
// ---------------------------------------

type SelectStmt struct {
	From  TableId
	Where *WhereClause
}

func (*SelectStmt) isStatement() {}

type WhereClause struct {
	Condition *BinaryExpr
}

// ---------------------------------------
// Insert
// ---------------------------------------

type InsertStmt struct {
	Table  TableId
	Cols   []ColumnId
	Values [][]Literal
}

func (*InsertStmt) isStatement() {}

// ---------------------------------------
// Delete
// ---------------------------------------

type DeleteStmt struct {
	From  TableId
	Where *WhereClause
}

func (*DeleteStmt) isStatement() {}

// ---------------------------------------
// Update
// ---------------------------------------

type UpdateStmt struct {
	Table      TableId
	SetClauses []*SetClause
	Where      *WhereClause
}

func (*UpdateStmt) isStatement() {}

type SetClause struct {
	Column ColumnId
	Value  Literal
}

// ---------------------------------------
// Transaction
// ---------------------------------------

type TransactionKind int

const (
	TxBegin TransactionKind = iota
	TxCommit
	TxRollback
)

type TransactionStmt struct {
	Kind TransactionKind
}

func (*TransactionStmt) isStatement() {}
