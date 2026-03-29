package executor

import (
	"minesql/internal/access"
	"minesql/internal/engine"
	"minesql/internal/undo"
)

// Insert はレコードを追加する
type Insert struct {
	trx     *Transaction
	table   *access.TableAccessMethod
	records []Record
}

func NewInsert(trx *Transaction, table *access.TableAccessMethod, records []Record) *Insert {
	return &Insert{
		trx:     trx,
		table:   table,
		records: records,
	}
}

func (ins *Insert) Next() (Record, error) {
	e := engine.Get()

	for _, record := range ins.records {
		ins.trx.AddUndoLogRecord(undo.NewInsertLogRecord(ins.table, record))
		if err := ins.table.Insert(e.BufferPool, record); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
