package executor

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/access"
)

type Update struct {
	trx           *access.Transaction
	table         *access.Table
	innerIterator RowIterator
	setColumn     Column // SET 句の内容
}

func NewUpdate(trx *access.Transaction, table *access.Table, inner RowIterator) *Update {
	return &Update{
		trx:           trx,
		table:         table,
		innerIterator: inner,
	}
}

func (u *Update) Execute() (int, error) {
	var affected int
	for {
		record, ok, err := u.innerIterator.Next()
		if err != nil {
			return 0, err
		}
		if !ok {
			break
		}
		switch r := record.(type) {
		case *access.PrimaryRecord:
			if err := u.table.Update(u.trx, r, u.setColumn.colNames, u.setColumn.values); err != nil {
				return 0, err
			}
			affected++
		default:
			return 0, errors.New("invalid record type")
		}
	}
	return affected, nil
}
