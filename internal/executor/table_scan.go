package executor

import (
	"minesql/internal/storage/engine"
)

// TableScan はテーブル全体を走査する
type TableScan struct {
	table          *engine.TableHandler
	whileCondition func(record Record) bool // 継続条件を満たすかどうかを判定する関数
	searchMode     engine.SearchMode
	iterator       engine.TableIterator
}

func NewTableScan(
	table *engine.TableHandler,
	searchMode engine.SearchMode,
	whileCondition func(record Record) bool,
) *TableScan {
	return &TableScan{
		table:          table,
		searchMode:     searchMode,
		whileCondition: whileCondition,
	}
}

func (ss *TableScan) Next() (Record, error) {
	e := engine.Get()

	// 初回実行時はイテレータを作成
	if ss.iterator == nil {
		iterator, err := ss.table.Search(e.BufferPool, ss.searchMode)
		if err != nil {
			return nil, err
		}

		ss.iterator = iterator
	}

	// レコード取得
	record, ok, err := ss.iterator.Next()
	if !ok {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// 継続条件をチェック
	if !ss.whileCondition(record) {
		return nil, nil
	}

	return record, nil
}
