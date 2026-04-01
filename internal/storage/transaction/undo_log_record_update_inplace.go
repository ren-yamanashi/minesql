package transaction

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/buffer"
)

type UndoUpdateInplaceRecord struct {
	table      *access.TableAccessMethod
	PrevRecord [][]byte // 更新前のレコード
	NewRecord  [][]byte // 更新後のレコード
}

func NewUndoUpdateInplaceRecord(table *access.TableAccessMethod, prevRecord, newRecord [][]byte) UndoUpdateInplaceRecord {
	return UndoUpdateInplaceRecord{
		table:      table,
		PrevRecord: prevRecord,
		NewRecord:  newRecord,
	}
}

// Undo は UpdateInplace したレコードを元の値に戻す
func (r UndoUpdateInplaceRecord) Undo(bp *buffer.BufferPool) error {
	// 元に戻すので、PrevRecord を新しい値、NewRecord を古い値として UpdateInplace を呼び出す
	return r.table.UpdateInplace(bp, r.NewRecord, r.PrevRecord)
}
