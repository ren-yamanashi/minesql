package executor

import (
	"minesql/internal/access"
	"minesql/internal/engine"
	"minesql/internal/undo"
)

// Delete は InnerExecutor の結果を元にレコードを削除する
type Delete struct {
	trx           *Transaction
	table         *access.TableAccessMethod
	InnerExecutor Executor
}

func NewDelete(trx *Transaction, table *access.TableAccessMethod, innerExecutor Executor) *Delete {
	return &Delete{
		trx:           trx,
		table:         table,
		InnerExecutor: innerExecutor,
	}
}

func (del *Delete) Next() (Record, error) {
	e := engine.Get()

	// 削除対象のレコードを先にすべて取得する
	// (削除により Iterator が参照するページデータが破壊されるのを防ぐ)
	var records []Record
	for {
		record, err := del.InnerExecutor.Next()
		if err != nil {
			return nil, err
		}
		if record == nil {
			break
		}
		records = append(records, record)
	}

	// 取得したレコードを削除
	for _, record := range records {
		del.trx.AddUndoLogRecord(undo.NewDeleteLogRecord(del.table, record))
		if err := del.table.SoftDelete(e.BufferPool, record); err != nil {
			return nil, err
		}
	}

	return nil, nil
}
