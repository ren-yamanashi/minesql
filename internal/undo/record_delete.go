package undo

import (
	"minesql/internal/engine"
	"minesql/internal/storage/access"
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
func (r DeleteLogRecord) Undo() error {
	return r.table.Insert(engine.Get().BufferPool, r.Record)
}
