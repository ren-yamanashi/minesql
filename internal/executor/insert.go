package executor

import "minesql/internal/storage/handler"

// Insert はレコードを追加する
type Insert struct {
	trxId   handler.TrxId
	table   *handler.TableHandler
	records []Record
}

func NewInsert(trxId handler.TrxId, table *handler.TableHandler, records []Record) *Insert {
	return &Insert{
		trxId:   trxId,
		table:   table,
		records: records,
	}
}

func (ins *Insert) Next() (Record, error) {
	e := handler.Get()

	for _, record := range ins.records {
		e.AppendInsertUndo(ins.trxId, ins.table, record)
		if err := ins.table.Insert(e.BufferPool, record); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
