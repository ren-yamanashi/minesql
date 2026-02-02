package definition

type DataType string

const (
	DataTypeVarchar DataType = "VARCHAR"
)

type ColumnDef struct {
	DefType  DefType
	ColName  string
	DataType DataType
}

func NewColumnDef(name string, dataType DataType) *ColumnDef {
	return &ColumnDef{
		DefType:  DefTypeColumn,
		ColName:  name,
		DataType: dataType,
	}
}

func (cd *ColumnDef) definitionNode() {}
