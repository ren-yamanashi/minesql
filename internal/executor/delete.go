package executor

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/access"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

type Delete struct {
	trxId         lock.TrxId
	table         *access.Table
	innerExecutor Executor
}

func NewDelete(trxId lock.TrxId, table *access.Table, inner Executor) *Delete {
	return &Delete{
		trxId:         trxId,
		table:         table,
		innerExecutor: inner,
	}
}

func (d *Delete) Next() (access.Record, error) {
	for {
		record, err := d.innerExecutor.Next()
		if err != nil {
			return nil, err
		}
		if record == nil {
			break
		}
		switch r := record.(type) {
		case *access.PrimaryRecord:
			if err := d.table.SoftDelete(r, d.trxId); err != nil {
				return nil, err
			}
		default:
			return nil, errors.New("invalid record type")
		}
	}
	return nil, nil
}
