package executor

import (
	"minesql/internal/access"
	"minesql/internal/engine"
)

type SearchTable struct {
	whileCondition func(record Record) bool // 継続条件を満たすかどうかを判定する関数
	iterator       *access.ClusteredIndexIterator
	tableName      string
	searchMode     access.RecordSearchMode
}

func NewSearchTable(
	tableName string,
	searchMode access.RecordSearchMode,
	whileCondition func(record Record) bool,
) *SearchTable {
	return &SearchTable{
		tableName:      tableName,
		searchMode:     searchMode,
		whileCondition: whileCondition,
	}
}

func (ss *SearchTable) Next() (Record, error) {
	sm := engine.Get()

	// 初回実行時はイテレータを作成
	if ss.iterator == nil {
		tblMeta, err := sm.Catalog.GetTableMetadataByName(ss.tableName)
		if err != nil {
			return nil, err
		}
		tbl, err := tblMeta.GetTable()
		if err != nil {
			return nil, err
		}

		iterator, err := tbl.Search(sm.BufferPool, ss.searchMode)
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
