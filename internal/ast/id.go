package ast

type IdType string

const (
	IdTypeColumn IdType = "column"
	IdTypeTable  IdType = "table"
)

// ========================
// Column
// ========================

type ColumnId struct {
	IdType  IdType
	ColName string
}

func NewColumnId(name string) *ColumnId {
	return &ColumnId{
		IdType:  IdTypeColumn,
		ColName: name,
	}
}

// ========================
// Table
// ========================

type TableId struct {
	IdType    IdType
	TableName string
}

func NewTableId(name string) *TableId {
	return &TableId{
		IdType:    IdTypeTable,
		TableName: name,
	}
}
