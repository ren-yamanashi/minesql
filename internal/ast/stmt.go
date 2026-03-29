package ast

const (
	StmtTypeCreate   StmtType = "create"
	StmtTypeInsert   StmtType = "insert"
	StmtTypeSelect   StmtType = "select"
	StmtTypeUpdate   StmtType = "update"
	StmtTypeDelete   StmtType = "delete"
	StmtTypeBegin    StmtType = "begin"
	StmtTypeCommit   StmtType = "commit"
	StmtTypeRollback StmtType = "rollback"
)

type StmtType string

type Statement interface{}

// ===========================
// Create Table
// ===========================

type CreateTableStmt struct {
	StmtType          StmtType
	Keyword           KeywordType
	TableName         string
	CreateDefinitions []Definition
}

type KeywordType string

const (
	KeywordTable KeywordType = "table"
)

// ===========================
// Select
// ===========================

type SelectStmt struct {
	StmtType StmtType
	From     TableId
	Where    *WhereClause
}

type WhereClause struct {
	Condition Expression
	IsSet     bool
}

// ===========================
// Insert
// ===========================

type InsertStmt struct {
	StmtType StmtType
	Table    TableId
	Cols     []ColumnId
	Values   [][]Literal
}

// ===========================
// Delete
// ===========================

type DeleteStmt struct {
	StmtType StmtType
	From     TableId
	Where    *WhereClause
}

// ===========================
// Update
// ===========================

type UpdateStmt struct {
	StmtType   StmtType
	Table      TableId
	SetClauses []*SetClause
	Where      *WhereClause
}

type SetClause struct {
	Column ColumnId
	Value  Literal
}

// ===========================
// Transaction
// ===========================

type BeginStmt struct {
	StmtType StmtType
}

type CommitStmt struct {
	StmtType StmtType
}

type RollbackStmt struct {
	StmtType StmtType
}
