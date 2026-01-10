package planner

import (
	"minesql/internal/executor"
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

type SequentialScan struct {
	TableMetaPageId disk.OldPageId
	SearchMode      btree.SearchMode
	// 継続条件を満たすかどうかを判定する関数
	WhileCondition func(record executor.Record) bool
}

func NewSequentialScan(
	tableMetaPageId disk.OldPageId,
	searchMode btree.SearchMode,
	whileCondition func(record executor.Record) bool,
) SequentialScan {
	return SequentialScan{
		TableMetaPageId: tableMetaPageId,
		SearchMode:      searchMode,
		WhileCondition:  whileCondition,
	}
}

// 実行計画を開始し、Executor を返す
func (ss *SequentialScan) Start(bpm *bufferpool.BufferPoolManager) (executor.Executor, error) {
	btr := btree.NewBTree(ss.TableMetaPageId)
	tableIterator, err := btr.Search(bpm, ss.SearchMode)
	if err != nil {
		return nil, err
	}
	return executor.NewSequentialScan(tableIterator, ss.WhileCondition), nil
}
