package transaction

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/lock"
)

type UndoUpdateInplaceRecord struct {
	table      *access.Table
	PrevRecord [][]byte // 更新前のレコード
	NewRecord  [][]byte // 更新後のレコード
}

func NewUndoUpdateInplaceRecord(table *access.Table, prevRecord, newRecord [][]byte) UndoUpdateInplaceRecord {
	return UndoUpdateInplaceRecord{
		table:      table,
		PrevRecord: prevRecord,
		NewRecord:  newRecord,
	}
}

// Undo は UpdateInplace したレコードを元の値に戻す
func (r UndoUpdateInplaceRecord) Undo(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager) error {
	// 元に戻すので、PrevRecord を新しい値、NewRecord を古い値として UpdateInplace を呼び出す
	return r.table.UpdateInplace(bp, trxId, lockMgr, r.NewRecord, r.PrevRecord)
}
