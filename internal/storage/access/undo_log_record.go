package access

import (
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/lock"
)

type LogRecord interface {
	Undo(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager) error
}
