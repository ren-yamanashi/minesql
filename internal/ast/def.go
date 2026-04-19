package ast

type Definition interface {
	isDefinition()
}

// --------------------------------
// Column
// --------------------------------

type DataType string

const (
	DataTypeVarchar DataType = "VARCHAR"
)

type ColumnDef struct {
	ColName  string
	DataType DataType
}

func (*ColumnDef) isDefinition() {}

// --------------------------------
// Constraint
// --------------------------------

type ConstraintPrimaryKeyDef struct {
	Columns []ColumnId
}

func (*ConstraintPrimaryKeyDef) isDefinition() {}

type ConstraintUniqueKeyDef struct {
	KeyName string
	Column  ColumnId
}

func (*ConstraintUniqueKeyDef) isDefinition() {}

type ConstraintKeyDef struct {
	KeyName string
	Column  ColumnId
}

func (*ConstraintKeyDef) isDefinition() {}
