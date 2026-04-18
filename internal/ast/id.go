package ast

// --------------------------------
// Column
// --------------------------------

type ColumnId struct {
	TableName string
	ColName   string
}

func NewColumnId(name string) *ColumnId {
	return &ColumnId{
		ColName: name,
	}
}

// --------------------------------
// Table
// --------------------------------

type TableId struct {
	TableName string
}

func NewTableId(name string) *TableId {
	return &TableId{
		TableName: name,
	}
}
