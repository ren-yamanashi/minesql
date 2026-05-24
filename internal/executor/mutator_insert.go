package executor

import (
	"github.com/ren-yamanashi/minesql/internal/storage/access"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

type Insert struct {
	trxId   lock.TrxId
	table   *access.Table
	columns []Column
}

func NewInsert(trxId lock.TrxId, table *access.Table, columns []Column) *Insert {
	return &Insert{
		trxId:   trxId,
		table:   table,
		columns: columns,
	}
}

func (i *Insert) Execute() (int, error) {
	for _, col := range i.columns {
		if err := i.table.Insert(col.colNames, col.values, i.trxId); err != nil {
			return 0, err
		}
	}
	return len(i.columns), nil
}
