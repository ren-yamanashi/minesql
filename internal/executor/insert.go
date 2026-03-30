package executor

import "minesql/internal/storage/engine"

// Insert はレコードを追加する
type Insert struct {
	trxId   engine.TrxId
	table   *engine.TableHandler
	records []Record
}

func NewInsert(trxId engine.TrxId, table *engine.TableHandler, records []Record) *Insert {
	return &Insert{
		trxId:   trxId,
		table:   table,
		records: records,
	}
}

func (ins *Insert) Next() (Record, error) {
	e := engine.Get()

	for _, record := range ins.records {
		e.AppendInsertUndo(ins.trxId, ins.table, record)
		if err := ins.table.Insert(e.BufferPool, record); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
