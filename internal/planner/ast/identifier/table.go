package identifier

type TableId struct {
	IdType     IdType
	TableName  string
	SchemaName string
}

func NewTableId(name string, schema string) *TableId {
	return &TableId{
		IdType:     IdTypeTable,
		TableName:  name,
		SchemaName: schema,
	}
}

func (t *TableId) identifierNode() {}
