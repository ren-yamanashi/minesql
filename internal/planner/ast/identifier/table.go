package identifier

type TableId struct {
	IdType     IdType
	TableName  string
}

func NewTableId(name string) *TableId {
	return &TableId{
		IdType:     IdTypeTable,
		TableName:  name,
	}
}

func (t *TableId) identifierNode() {}
