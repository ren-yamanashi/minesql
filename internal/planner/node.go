package planner

import "minesql/internal/executor"

type Node interface {
	Start() executor.Executor
}
