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
	indexOnly      bool // true の場合、primary lookup をスキップし index データのみで結果を返す
	nCols          int  // テーブルのカラム数 (indexOnly 時のレコード構築用)
	ukColPos       int  // ユニークキーのカラム位置 (indexOnly 時のレコード構築用)
	iterator       *access.UniqueIndexIterator
}

type IndexScanParams struct {
	Table          *access.Table
	Index          *access.UniqueIndex
	SearchMode     access.RecordSearchMode
	WhileCondition func(Record) bool
	IndexOnly      bool
	NCols          int
	UKColPos       int
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

func NewIndexScanWithParams(params IndexScanParams) *IndexScan {
	return &IndexScan{
		table:          params.Table,
		index:          params.Index,
		searchMode:     params.SearchMode,
		whileCondition: params.WhileCondition,
		indexOnly:      params.IndexOnly,
		nCols:          params.NCols,
		ukColPos:       params.UKColPos,
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

	if is.indexOnly {
		return is.nextIndexOnly()
	}
	return is.nextWithPrimaryLookup()
}

// nextWithPrimaryLookup はインデックスから取得後 primary lookup して全カラムを返す (従来の動作)
func (is *IndexScan) nextWithPrimaryLookup() (Record, error) {
	result, ok, err := is.iterator.Next()
	if !ok {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !is.whileCondition(result.UniqueKey) {
		return nil, nil
	}
	return result.Record, nil
}

// nextIndexOnly はインデックスデータのみからレコードを構築する (primary lookup なし)
//
// PK カラムはインデックスキーに含まれるため取得可能。UK カラムも同様。
// その他のカラムは nil を設定し、Project で必要なカラムのみ取り出す
func (is *IndexScan) nextIndexOnly() (Record, error) {
	result, ok, err := is.iterator.NextIndexOnly()
	if !ok {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !is.whileCondition(result.UniqueKey) {
		return nil, nil
	}

	// テーブル全カラム幅のレコードを構築 (Project がカラム位置で取り出せるように)
	record := make(Record, is.nCols)
	// PK カラム (先頭から pkCount 個)
	copy(record, result.PKValues)
	// UK カラム
	if is.ukColPos < is.nCols && len(result.UniqueKey) > 0 {
		record[is.ukColPos] = result.UniqueKey[0]
	}

	return record, nil
}
