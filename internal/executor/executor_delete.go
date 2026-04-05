package executor

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
)

// Delete は InnerExecutor の結果を元にレコードを削除する
type Delete struct {
	trxId         handler.TrxId
	table         *access.Table
	innerExecutor Executor
}

func NewDelete(trxId handler.TrxId, table *access.Table, innerExecutor Executor) *Delete {
	return &Delete{
		trxId:         trxId,
		table:         table,
		innerExecutor: innerExecutor,
	}
}

func (del *Delete) Next() (Record, error) {
	h := handler.Get()

	// 削除対象のレコードを先にすべて取得する
	// (削除により Iterator が参照するページデータが破壊されるのを防ぐ)
	var records []Record
	for {
		record, err := del.innerExecutor.Next()
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
		h.AppendDeleteUndo(del.trxId, del.table, record)
		if err := del.table.SoftDelete(h.BufferPool, del.trxId, h.LockMgr, record); err != nil {
			h.UndoLog().PopLast(del.trxId)
			return nil, err
		}
	}

	return nil, nil
}
