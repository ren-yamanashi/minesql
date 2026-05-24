package executor

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/access"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

type Delete struct {
	trxId         lock.TrxId
	table         *access.Table
	innerIterator RowIterator
}

func NewDelete(trxId lock.TrxId, table *access.Table, inner RowIterator) *Delete {
	return &Delete{
		trxId:         trxId,
		table:         table,
		innerIterator: inner,
	}
}

func (d *Delete) Execute() (int, error) {
	var affected int
	for {
		record, ok, err := d.innerIterator.Next()
		if err != nil {
			return 0, err
		}
		if !ok {
			break
		}
		switch r := record.(type) {
		case *access.PrimaryRecord:
			if err := d.table.SoftDelete(r, d.trxId); err != nil {
				return 0, err
			}
			affected++
		default:
			return 0, errors.New("invalid record type")
		}
	}
	return affected, nil
}
