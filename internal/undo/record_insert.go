package undo

import (
	"minesql/internal/access"
	"minesql/internal/engine"
)

type InsertLogRecord struct {
	table  *access.TableAccessMethod
	Record [][]byte
}

func NewInsertLogRecord(table *access.TableAccessMethod, record [][]byte) InsertLogRecord {
	return InsertLogRecord{
		table:  table,
		Record: record,
	}
}

// Undo は Insert したレコードを物理削除する
func (r InsertLogRecord) Undo() error {
	return r.table.Delete(engine.Get().BufferPool, r.Record)
}
