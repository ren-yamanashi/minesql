package planner

import (
	"minesql/internal/executor"
	"minesql/internal/storage/bufferpool"
)

type Filter struct {
	InnerPlanNode Node
	Condition     func(executor.Record) bool
}

func NewFilter(innerPlanNode Node, condition func(executor.Record) bool) Filter {
	return Filter{
		InnerPlanNode: innerPlanNode,
		Condition:     condition,
	}
}

// 実行計画を開始し、Executor を返す
func (f *Filter) Start(bpm *bufferpool.BufferPoolManager) (executor.Executor, error) {
	innerIterator, err := f.InnerPlanNode.Start(bpm)
	if err != nil {
		return nil, err
	}
	return executor.NewFilter(innerIterator, f.Condition), nil
}
