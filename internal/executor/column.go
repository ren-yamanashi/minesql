package executor

type Column struct {
	colNames []string
	values   []string
}

func NewColumn(colNames, values []string) Column {
	return Column{colNames: colNames, values: values}
}

func (c Column) ColName() []string { return c.colNames }
func (c Column) Values() []string  { return c.values }
