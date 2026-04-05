package executor

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
)

// Insert はレコードを追加する
type Insert struct {
	trxId   handler.TrxId
	table   *access.Table
	records []Record
}

func NewInsert(trxId handler.TrxId, table *access.Table, records []Record) *Insert {
	return &Insert{
		trxId:   trxId,
		table:   table,
		records: records,
	}
}

func (ins *Insert) Next() (Record, error) {
	hdl := handler.Get()

	for _, record := range ins.records {
		if err := ins.table.Insert(hdl.BufferPool, ins.trxId, hdl.LockMgr, record); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
