package transaction

import (
	"minesql/internal/storage/buffer"
)

type UpdateInplaceLogRecord struct {
	table      TableOperator
	PrevRecord [][]byte // 更新前のレコード
	NewRecord  [][]byte // 更新後のレコード
}

func NewUpdateInplaceLogRecord(table TableOperator, prevRecord, newRecord [][]byte) UpdateInplaceLogRecord {
	return UpdateInplaceLogRecord{
		table:      table,
		PrevRecord: prevRecord,
		NewRecord:  newRecord,
	}
}

// Undo は UpdateInplace したレコードを元の値に戻す
func (r UpdateInplaceLogRecord) Undo(bp *buffer.BufferPool) error {
	// 元に戻すので、PrevRecord を新しい値、NewRecord を古い値として UpdateInplace を呼び出す
	return r.table.UpdateInplace(bp, r.NewRecord, r.PrevRecord)
}
