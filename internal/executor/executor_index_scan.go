package executor

import (
	"minesql/internal/storage/handler"
	"minesql/internal/storage/transaction"
)

// IndexScan はセカンダリインデックスを利用して検索する
type IndexScan struct {
	table          *transaction.Table
	index          *transaction.UniqueIndex
	searchMode     transaction.RecordSearchMode
	whileCondition func(Record) bool
	iterator       *transaction.UniqueIndexIterator
}

func NewIndexScan(
	table *transaction.Table,
	index *transaction.UniqueIndex,
	searchMode transaction.RecordSearchMode,
	whileCondition func(record Record) bool,
) *IndexScan {
	return &IndexScan{
		table:          table,
		index:          index,
		searchMode:     searchMode,
		whileCondition: whileCondition,
	}
}

func (is *IndexScan) Next() (Record, error) {
	hdl := handler.Get()

	// 初回実行時にイテレータを作成
	if is.iterator == nil {
		iter, err := is.index.Search(hdl.BufferPool, is.table, is.searchMode)
		if err != nil {
			return nil, err
		}
		is.iterator = iter
	}

	// インデックスから次の結果を取得
	result, ok, err := is.iterator.Next()
	if !ok {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// 継続条件をチェック (セカンダリキーで判定)
	if !is.whileCondition(result.UniqueKey) {
		return nil, nil
	}

	return result.Record, nil
}
