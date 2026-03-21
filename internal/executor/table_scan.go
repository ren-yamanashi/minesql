package executor

import (
	"minesql/internal/access"
	"minesql/internal/engine"
)

// TableScan はテーブル全体を走査する
type TableScan struct {
	table          *access.TableAccessMethod
	whileCondition func(record Record) bool // 継続条件を満たすかどうかを判定する関数
	searchMode     access.RecordSearchMode
	iterator       *access.ClusteredIndexIterator
}

func NewTableScan(
	table *access.TableAccessMethod,
	searchMode access.RecordSearchMode,
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
