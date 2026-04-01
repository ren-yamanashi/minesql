package transaction

import (
	"minesql/internal/storage/buffer"
)

type DeleteLogRecord struct {
	table  TableOperator
	Record [][]byte
}

func NewDeleteLogRecord(table TableOperator, record [][]byte) DeleteLogRecord {
	return DeleteLogRecord{
		table:  table,
		Record: record,
	}
}

// Undo は Delete したレコードを挿入する
func (r DeleteLogRecord) Undo(bp *buffer.BufferPool) error {
	return r.table.Insert(bp, r.Record)
}
