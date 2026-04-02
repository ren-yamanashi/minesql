package executor

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
)

// IndexScan はセカンダリインデックスを利用して検索する
type IndexScan struct {
	table          *access.Table
	index          *access.UniqueIndex
	searchMode     access.RecordSearchMode
	whileCondition func(Record) bool
	iterator       *access.UniqueIndexIterator
}

func NewIndexScan(
	table *access.Table,
	index *access.UniqueIndex,
	searchMode access.RecordSearchMode,
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
	e := handler.Get()

	// 初回実行時にイテレータを作成
	if is.iterator == nil {
		iter, err := is.index.Search(e.BufferPool, is.table, is.searchMode)
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
