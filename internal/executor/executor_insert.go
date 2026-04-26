package executor

import (
	"github.com/ren-yamanashi/minesql/internal/storage/access"
	"github.com/ren-yamanashi/minesql/internal/storage/handler"
)

// Insert はレコードを追加する
type Insert struct {
	trxId   handler.TrxId
	table   *access.Table
	records []Record
}

func NewInsert(trxId handler.TrxId, table *access.Table, records []Record) *Insert {
	return &Insert{
		trxId:   trxId,
		table:   table,
		records: records,
	}
}

func (ins *Insert) Next() (Record, error) {
	hdl := handler.Get()

	// テーブルの FK 制約を確認
	tableMeta, _ := hdl.Catalog.GetTableMetaByName(ins.table.Name)

	for _, record := range ins.records {
		// FK チェック: 参照先テーブルに値が存在するか確認 + Shared Lock 取得
		if tableMeta != nil {
			if err := checkFKOnInsert(hdl.BufferPool, ins.trxId, hdl.LockMgr, tableMeta, record); err != nil {
				return nil, err
			}
		}

		if err := ins.table.Insert(hdl.BufferPool, ins.trxId, hdl.LockMgr, record); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
