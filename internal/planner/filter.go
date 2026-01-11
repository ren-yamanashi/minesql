package planner

import "minesql/internal/executor"

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
func (f Filter) Start() executor.Executor {
	innerExecutor := f.InnerPlanNode.Start()
	return executor.NewFilter(innerExecutor, f.Condition)
}
