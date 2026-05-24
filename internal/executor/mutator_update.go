package executor

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/access"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

type Update struct {
	trxId         lock.TrxId
	table         *access.Table
	innerIterator RowIterator
	setColumn     Column // SET 句の内容
}

func NewUpdate(trxId lock.TrxId, table *access.Table, inner RowIterator) *Update {
	return &Update{
		trxId:         trxId,
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
			if err := u.table.Update(r, u.setColumn.colNames, u.setColumn.values, u.trxId); err != nil {
				return 0, err
			}
			affected++
		default:
			return 0, errors.New("invalid record type")
		}
	}
	return affected, nil
}
