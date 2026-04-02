package executor

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
)

// TableScan はテーブル全体を走査する
type TableScan struct {
	table          *access.Table
	searchMode     access.RecordSearchMode
	whileCondition func(Record) bool
	iterator       *access.TableIterator
}

func NewTableScan(
	table *access.Table,
	searchMode access.RecordSearchMode,
	whileCondition func(Record) bool,
) *TableScan {
	return &TableScan{
		table:          table,
		searchMode:     searchMode,
		whileCondition: whileCondition,
	}
}

func (ss *TableScan) Next() (Record, error) {
	hdl := handler.Get()

	// 初回実行時はイテレータを作成
	if ss.iterator == nil {
		iterator, err := ss.table.Search(hdl.BufferPool, ss.searchMode)
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
