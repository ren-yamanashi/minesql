package executor

import (
	"github.com/ren-yamanashi/minesql/internal/storage/access"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

type Update struct {
	trxId         lock.TrxId
	table         *access.Table
	innerExecutor Executor
}

func NewUpdate(trxId lock.TrxId, table *access.Table, inner Executor) *Update {
	return &Update{
		trxId:         trxId,
		table:         table,
		innerExecutor: inner,
	}
}

func (u *Update) Next() (access.Record, error) {
	// 更新対象のレコードを先位に全て収集する
	// (更新により Iterator が参照するページデータが破棄されるのを防ぐ)
	var records []access.Record
	for {
		record, err := u.innerExecutor.Next()
		if err != nil {
			return nil, err
		}
		if record == nil {
			break
		}
		records = append(records, record)
	}

	return nil, nil
}
