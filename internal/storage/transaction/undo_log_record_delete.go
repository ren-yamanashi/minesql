package transaction

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/lock"
)

type UndoDeleteRecord struct {
	table  *access.Table
	Record [][]byte
}

func NewUndoDeleteRecord(table *access.Table, record [][]byte) UndoDeleteRecord {
	return UndoDeleteRecord{
		table:  table,
		Record: record,
	}
}

// Undo は Delete したレコードを挿入する
func (r UndoDeleteRecord) Undo(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager) error { //nolint:revive // interface 準拠のため引数を受け取るが Insert にはロック不要
	return r.table.Insert(bp, r.Record)
}
