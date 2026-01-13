package identifier

type ColumnId struct {
	IdType IdType
	ColName string
}

func NewColumnId(name string) *ColumnId {
	return &ColumnId{
		IdType: TypeColumn,
		ColName: name,
	}
}

func (c *ColumnId) identifierNode() {}
