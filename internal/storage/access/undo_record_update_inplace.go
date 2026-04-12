package access

import (
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/lock"
)

type UndoUpdateInplaceRecord struct {
	table      *Table
	PrevRecord [][]byte // 更新前のレコード
	NewRecord  [][]byte // 更新後のレコード
}

func NewUndoUpdateInplaceRecord(table *Table, prevRecord, newRecord [][]byte) UndoUpdateInplaceRecord {
	return UndoUpdateInplaceRecord{
		table:      table,
		PrevRecord: prevRecord,
		NewRecord:  newRecord,
	}
}

// Undo は UpdateInplace したレコードを元の値に戻す
func (r UndoUpdateInplaceRecord) Undo(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager) error {
	// 元に戻すので、PrevRecord を新しい値、NewRecord を古い値として UpdateInplace を呼び出す
	return r.table.updateInplace(bp, trxId, lockMgr, r.NewRecord, r.PrevRecord)
}

// Serialize は UndoUpdateInplaceRecord をバイト列にシリアライズする
func (r UndoUpdateInplaceRecord) Serialize(trxId uint64, undoNo uint64) []byte {
	return SerializeUndoRecord(trxId, undoNo, UndoUpdateInplace, r.table.Name, r.PrevRecord, r.NewRecord)
}
