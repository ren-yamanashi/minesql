package executor

import (
	"minesql/internal/engine"
	"minesql/internal/storage/access"
	"minesql/internal/undo"
)

// Insert はレコードを追加する
type Insert struct {
	undoLog *undo.UndoLog
	trxId   undo.TrxId
	table   *access.TableAccessMethod
	records []Record
}

func NewInsert(undoLog *undo.UndoLog, trxId undo.TrxId, table *access.TableAccessMethod, records []Record) *Insert {
	return &Insert{
		undoLog: undoLog,
		trxId:   trxId,
		table:   table,
		records: records,
	}
}

func (ins *Insert) Next() (Record, error) {
	e := engine.Get()

	for _, record := range ins.records {
		ins.undoLog.Append(ins.trxId, undo.NewInsertLogRecord(ins.table, record))
		if err := ins.table.Insert(e.BufferPool, record); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
