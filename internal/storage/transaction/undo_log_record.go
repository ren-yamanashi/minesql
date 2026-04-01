package transaction

import "minesql/internal/storage/buffer"

type LogRecord interface {
	Undo(bp *buffer.BufferPool) error
}
