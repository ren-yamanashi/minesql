package planner

import (
	"minesql/internal/executor"
	"minesql/internal/storage/bufferpool"
)

type Node interface {
	Start(bpm *bufferpool.BufferPoolManager) (executor.Executor, error)
}
