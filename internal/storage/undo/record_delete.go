package undo

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/buffer"
)

type DeleteLogRecord struct {
	table  *access.TableAccessMethod
	Record [][]byte
}

func NewDeleteLogRecord(table *access.TableAccessMethod, record [][]byte) DeleteLogRecord {
	return DeleteLogRecord{
		table:  table,
		Record: record,
	}
}

// Undo は Delete したレコードを挿入する
func (r DeleteLogRecord) Undo(bp *buffer.BufferPool) error {
	return r.table.Insert(bp, r.Record)
}
