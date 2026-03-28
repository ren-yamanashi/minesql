package undo

import (
	"minesql/internal/access"
	"minesql/internal/engine"
)

type DeleteLogRecord struct {
	table  *access.TableAccessMethod
	Record [][]byte
}

// Undo は Delete したレコードを挿入する
func (r DeleteLogRecord) Undo() error {
	return r.table.Insert(engine.Get().BufferPool, r.Record)
}
