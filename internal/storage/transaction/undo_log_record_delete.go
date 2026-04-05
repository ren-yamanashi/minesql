package transaction

import (
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/lock"
)

type UndoDeleteRecord struct {
	table  *Table
	Record [][]byte
}

func NewUndoDeleteRecord(table *Table, record [][]byte) UndoDeleteRecord {
	return UndoDeleteRecord{
		table:  table,
		Record: record,
	}
}

// Undo は Delete したレコードを挿入する
func (r UndoDeleteRecord) Undo(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager) error {
	return r.table.insertRaw(bp, trxId, lockMgr, r.Record)
}
