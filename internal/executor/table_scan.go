package executor

import (
	"minesql/internal/storage/handler"
)

// TableScan はテーブル全体を走査する
type TableScan struct {
	table          *handler.TableHandler
	whileCondition func(record Record) bool // 継続条件を満たすかどうかを判定する関数
	searchMode     handler.SearchMode
	iterator       handler.TableIterator
}

func NewTableScan(
	table *handler.TableHandler,
	searchMode handler.SearchMode,
	whileCondition func(record Record) bool,
) *TableScan {
	return &TableScan{
		table:          table,
		searchMode:     searchMode,
		whileCondition: whileCondition,
	}
}

func (ss *TableScan) Next() (Record, error) {
	e := handler.Get()

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
