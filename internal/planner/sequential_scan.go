package planner

import (
	"minesql/internal/executor"
)

type SequentialScan struct {
	TableName      string
	SearchMode     executor.RecordSearchMode
	WhileCondition func(record executor.Record) bool
}

func NewSequentialScan(
	tableName string,
	searchMode executor.RecordSearchMode,
	whileCondition func(record executor.Record) bool,
) SequentialScan {
	return SequentialScan{
		TableName:      tableName,
		SearchMode:     searchMode,
		WhileCondition: whileCondition,
	}
}

// 実行計画を開始し、Executor を返す
func (ss SequentialScan) Start() executor.Executor {
	return executor.NewSequentialScan(
		ss.TableName,
		ss.SearchMode,
		ss.WhileCondition,
	)
}
