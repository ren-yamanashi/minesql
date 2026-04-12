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
	return r.table.delete(bp, trxId, lockMgr, r.Record) // delete は UndoPtr 不要 (物理削除)
}

// Serialize は UndoInsertRecord をバイト列にシリアライズする
func (r UndoInsertRecord) Serialize(trxId uint64, undoNo uint64) []byte {
	return SerializeUndoRecord(trxId, undoNo, UndoInsert, r.table.Name, r.Record)
}
