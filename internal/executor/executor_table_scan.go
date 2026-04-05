package executor

import (
	"minesql/internal/storage/handler"
	"minesql/internal/storage/lock"
	"minesql/internal/storage/transaction"
)

// TableScan はテーブル全体を走査する
type TableScan struct {
	trxId          handler.TrxId
	lockMgr        *lock.Manager
	table          *transaction.Table
	searchMode     transaction.RecordSearchMode
	whileCondition func(Record) bool
	iterator       *transaction.TableIterator
}

func NewTableScan(
	trxId handler.TrxId,
	lockMgr *lock.Manager,
	table *transaction.Table,
	searchMode transaction.RecordSearchMode,
	whileCondition func(Record) bool,
) *TableScan {
	return &TableScan{
		trxId:          trxId,
		lockMgr:        lockMgr,
		table:          table,
		searchMode:     searchMode,
		whileCondition: whileCondition,
	}
}

func (ss *TableScan) Next() (Record, error) {
	hdl := handler.Get()

	// 初回実行時はイテレータを作成
	if ss.iterator == nil {
		iterator, err := ss.table.Search(hdl.BufferPool, ss.trxId, ss.lockMgr, ss.searchMode)
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
