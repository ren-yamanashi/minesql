package planner

import (
	"minesql/internal/executor"
)

type IndexScan struct {
	TableName      string
	IndexName      string
	SearchMode     executor.RecordSearchMode
	WhileCondition func(record executor.Record) bool
}

func NewIndexScan(
	tableName string,
	indexName string,
	searchMode executor.RecordSearchMode,
	whileCondition func(record executor.Record) bool,
) IndexScan {
	return IndexScan{
		TableName:      tableName,
		IndexName:      indexName,
		SearchMode:     searchMode,
		WhileCondition: whileCondition,
	}
}

// 実行計画を開始し、Executor を返す
func (is IndexScan) Start() executor.Executor {
	return executor.NewIndexScan(
		is.TableName,
		is.IndexName,
		is.SearchMode,
		is.WhileCondition,
	)
}
