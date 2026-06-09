package executor

import (
	"github.com/ren-yamanashi/minesql/internal/storage/access"
)

type Insert struct {
	trx     *access.Transaction
	table   *access.Table
	columns []Column
}

func NewInsert(trx *access.Transaction, table *access.Table, columns []Column) *Insert {
	return &Insert{
		trx:     trx,
		table:   table,
		columns: columns,
	}
}

func (i *Insert) Execute() (int, error) {
	for _, col := range i.columns {
		if err := i.table.Insert(i.trx, col.colNames, col.values); err != nil {
			return 0, err
		}
	}
	return len(i.columns), nil
}
