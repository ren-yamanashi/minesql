package access

import (
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/lock"
)

type UndoInsertRecord struct {
	table  *Table
	Record [][]byte
}

func NewUndoInsertRecord(table *Table, record [][]byte) UndoInsertRecord {
	return UndoInsertRecord{
		table:  table,
		Record: record,
	}
}

// Undo は Insert したレコードを物理削除する
func (r UndoInsertRecord) Undo(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager) error {
	return r.table.deleteRaw(bp, trxId, lockMgr, r.Record)
}
