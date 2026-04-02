package transaction

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/buffer"
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
func (r UndoDeleteRecord) Undo(bp *buffer.BufferPool) error {
	return r.table.Insert(bp, r.Record)
}
