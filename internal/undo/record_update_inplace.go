package undo

import (
	"minesql/internal/access"
	"minesql/internal/engine"
)

type UpdateInplaceLogRecord struct {
	table      *access.TableAccessMethod
	PrevRecord [][]byte // 更新前のレコード
	NewRecord  [][]byte // 更新後のレコード
}

// Undo は UpdateInplace したレコードを元の値に戻す
func (r UpdateInplaceLogRecord) Undo() error {
	// 元に戻すので、PrevRecord を新しい値、NewRecord を古い値として UpdateInplace を呼び出す
	return r.table.UpdateInplace(engine.Get().BufferPool, r.NewRecord, r.PrevRecord)
}
