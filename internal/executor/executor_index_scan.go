package executor

import (
	"github.com/ren-yamanashi/minesql/internal/storage/access"
	"github.com/ren-yamanashi/minesql/internal/storage/handler"
)

// IndexScan はセカンダリインデックスを利用して検索する
type IndexScan struct {
	table          *access.Table
	index          *access.SecondaryIndex
	searchMode     access.RecordSearchMode
	whileCondition func(Record) bool
	indexOnly      bool // true の場合、テーブル本体の検索をスキップし index データのみで結果を返す
	nCols          int  // テーブルのカラム数 (indexOnly 時のレコード構築用)
	secColPos      int  // セカンダリキーのカラム位置 (indexOnly 時のレコード構築用)
	iterator       *access.SecondaryIndexIterator
}

type IndexScanParams struct {
	Table          *access.Table
	Index          *access.SecondaryIndex
	SearchMode     access.RecordSearchMode
	WhileCondition func(Record) bool
	IndexOnly      bool
	NCols          int
	SecColPos      int
}

func NewIndexScan(
	table *access.Table,
	index *access.SecondaryIndex,
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
		secColPos:      params.SecColPos,
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

// nextWithPrimaryLookup はインデックスから取得後、PK でテーブル本体を検索して全カラムを返す (従来の動作)
func (is *IndexScan) nextWithPrimaryLookup() (Record, error) {
	result, ok, err := is.iterator.Next()
	if !ok {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !is.whileCondition(result.SecondaryKey) {
		return nil, nil
	}
	return result.Record, nil
}

// nextIndexOnly はインデックスデータのみからレコードを構築する (テーブル本体の検索なし)
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
	if !is.whileCondition(result.SecondaryKey) {
		return nil, nil
	}

	// テーブル全カラム幅のレコードを構築 (Project がカラム位置で取り出せるように)
	record := make(Record, is.nCols)
	// PK カラム (先頭から pkCount 個)
	copy(record, result.PKValues)
	// UK カラム
	if is.secColPos < is.nCols && len(result.SecondaryKey) > 0 {
		record[is.secColPos] = result.SecondaryKey[0]
	}

	return record, nil
}
