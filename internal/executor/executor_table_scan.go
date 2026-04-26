package executor

import (
	"github.com/ren-yamanashi/minesql/internal/storage/access"
	"github.com/ren-yamanashi/minesql/internal/storage/handler"
)

// TableScanParams は NewTableScan の引数
type TableScanParams struct {
	ReadView       *access.ReadView
	VersionReader  *access.VersionReader
	Table          *access.Table
	SearchMode     access.RecordSearchMode
	WhileCondition func(Record) bool
}

// TableScan はテーブル全体を走査する
type TableScan struct {
	readView       *access.ReadView
	versionReader  *access.VersionReader
	table          *access.Table
	searchMode     access.RecordSearchMode
	whileCondition func(Record) bool
	iterator       *access.TableIterator
}

func NewTableScan(params TableScanParams) *TableScan {
	return &TableScan{
		readView:       params.ReadView,
		versionReader:  params.VersionReader,
		table:          params.Table,
		searchMode:     params.SearchMode,
		whileCondition: params.WhileCondition,
	}
}

func (ss *TableScan) Next() (Record, error) {
	hdl := handler.Get()

	// 初回実行時はイテレータを作成
	if ss.iterator == nil {
		iterator, err := ss.table.Search(
			hdl.BufferPool,
			ss.readView,
			ss.versionReader,
			ss.searchMode,
		)
		if err != nil {
			return nil, err
		}

		ss.iterator = iterator
	}

	// レコード取得
	record, ok, err := ss.iterator.Next()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	// 継続条件をチェック
	if !ss.whileCondition(record) {
		return nil, nil
	}

	return record, nil
}
