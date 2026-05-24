package executor

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/access"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

type SetColumn struct {
	colNames []string
	value    []string
}

type Update struct {
	trxId         lock.TrxId
	table         *access.Table
	innerExecutor Executor
	setColumn     SetColumn // SET 句の内容
}

func NewUpdate(trxId lock.TrxId, table *access.Table, inner Executor) *Update {
	return &Update{
		trxId:         trxId,
		table:         table,
		innerExecutor: inner,
	}
}

func (u *Update) Next() (access.Record, error) {
	// 更新対象のレコードを収集
	var records []*access.PrimaryRecord
	for {
		record, err := u.innerExecutor.Next()
		if err != nil {
			return nil, err
		}
		if record == nil {
			break
		}
		switch r := record.(type) {
		case *access.PrimaryRecord:
			records = append(records, r)
		default:
			return nil, errors.New("invalid record type")
		}
	}

	// 更新
	for _, record := range records {
		if err := u.table.Update(record, u.setColumn.colNames, u.setColumn.value, u.trxId); err != nil {
			return nil, err
		}
	}

	return nil, nil
}
