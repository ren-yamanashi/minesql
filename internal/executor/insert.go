package executor

import (
	"minesql/internal/engine"
	"minesql/internal/storage/access"
	"minesql/internal/storage/transaction"
)

// Insert はレコードを追加する
type Insert struct {
	trxId   engine.TrxId
	table   *access.TableAccessMethod
	records []Record
}

func NewInsert(trxId engine.TrxId, table *access.TableAccessMethod, records []Record) *Insert {
	return &Insert{
		trxId:   trxId,
		table:   table,
		records: records,
	}
}

func (ins *Insert) Next() (Record, error) {
	e := engine.Get()

	for _, record := range ins.records {
		e.UndoLog().Append(ins.trxId, transaction.NewInsertLogRecord(ins.table, record))
		if err := ins.table.Insert(e.BufferPool, record); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
