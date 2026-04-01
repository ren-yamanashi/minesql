package transaction

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/buffer"
)

type UndoInsertRecord struct {
	table  *access.TableAccessMethod
	Record [][]byte
}

func NewUndoInsertRecord(table *access.TableAccessMethod, record [][]byte) UndoInsertRecord {
	return UndoInsertRecord{
		table:  table,
		Record: record,
	}
}

// Undo は Insert したレコードを物理削除する
func (r UndoInsertRecord) Undo(bp *buffer.BufferPool) error {
	return r.table.Delete(bp, r.Record)
}
