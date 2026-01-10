package planner

import (
	"minesql/internal/executor"
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

type IndexScan struct {
	TableMetaPageId disk.OldPageId
	IndexMetaPageId disk.OldPageId
	SearchMode      btree.SearchMode
	// 継続条件を満たすかどうかを判定する関数
	WhileCondition func(record executor.Record) bool
}

func NewIndexScan(
	tableMetaPageId disk.OldPageId,
	indexMetaPageId disk.OldPageId,
	searchMode btree.SearchMode,
	whileCondition func(record executor.Record) bool,
) IndexScan {
	return IndexScan{
		TableMetaPageId: tableMetaPageId,
		IndexMetaPageId: indexMetaPageId,
		SearchMode:      searchMode,
		WhileCondition:  whileCondition,
	}
}

// 実行計画を開始し、Executor を返す
func (is *IndexScan) Start(bpm *bufferpool.BufferPoolManager) (executor.Executor, error) {
	indexBtr := btree.NewBTree(is.IndexMetaPageId)
	indexIterator, err := indexBtr.Search(bpm, is.SearchMode)
	if err != nil {
		return nil, err
	}
	return executor.NewIndexScan(
		is.TableMetaPageId,
		indexIterator,
		is.WhileCondition,
	), nil
}
