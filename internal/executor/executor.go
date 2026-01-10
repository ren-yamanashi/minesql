package executor

import "minesql/internal/storage/bufferpool"

type Executor interface {
	Next(bpm *bufferpool.BufferPoolManager) (Record, error)
}
