package executor

import (
	"minesql/internal/access"
	"minesql/internal/engine"
)

type SearchIndex struct {
	executor
	tableName      string
	indexName      string
	searchMode     access.RecordSearchMode
	whileCondition func(record Record) bool // 継続条件を満たすかどうかを判定する関数
	iterator       *access.SecondaryIndexIterator
}

func NewSearchIndex(
	tableName string,
	indexName string,
	searchMode access.RecordSearchMode,
	whileCondition func(record Record) bool,
) *SearchIndex {
	return &SearchIndex{
		tableName:      tableName,
		indexName:      indexName,
		searchMode:     searchMode,
		whileCondition: whileCondition,
	}
}

func (is *SearchIndex) Next() (Record, error) {
	sm := engine.Get()

	// 初回実行時にイテレータを作成
	if is.iterator == nil {
		tblMeta, err := sm.Catalog.GetTableMetadataByName(is.tableName)
		if err != nil {
			return nil, err
		}
		tbl, err := tblMeta.GetTable()
		if err != nil {
			return nil, err
		}
		index, err := tbl.GetUniqueIndexByName(is.indexName)
		if err != nil {
			return nil, err
		}

		iter, err := index.Search(sm.BufferPool, tbl, is.searchMode)
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
	if !is.whileCondition(result.SecondaryKey) {
		return nil, nil
	}

	return result.Record, nil
}
