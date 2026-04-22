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

type ConstraintForeignKeyDef struct {
	KeyName   string   // FK 制約名 (必須)
	Column    ColumnId // FK カラム
	RefTable  string   // 参照先テーブル名
	RefColumn string   // 参照先カラム名
}

func (*ConstraintForeignKeyDef) isDefinition() {}
