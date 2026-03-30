package transaction

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/buffer"
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
func (r InsertLogRecord) Undo(bp *buffer.BufferPool) error {
	return r.table.Delete(bp, r.Record)
}
