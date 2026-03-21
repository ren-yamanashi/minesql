package ast

type DefType string

const (
	DefTypeConstraintPrimaryKey DefType = "constraint primary key"
	DefTypeConstraintUniqueKey  DefType = "constraint unique key"
	DefTypeColumn               DefType = "column"
)

type Definition interface{}

// ========================
// Column
// ========================

type DataType string

const (
	DataTypeVarchar DataType = "VARCHAR"
)

type ColumnDef struct {
	DefType  DefType
	ColName  string
	DataType DataType
}

// ========================
// Constraint
// ========================

type ConstraintPrimaryKeyDef struct {
	DefType DefType
	Columns []ColumnId
}

type ConstraintUniqueKeyDef struct {
	DefType DefType
	KeyName string
	Column  ColumnId
}
