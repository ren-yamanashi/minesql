package executor

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/handler"
	"minesql/internal/storage/lock"
)

// Delete は InnerExecutor の結果を元にレコードを削除する
type Delete struct {
	trxId         handler.TrxId
	table         *access.Table
	innerExecutor Executor
}

func NewDelete(trxId handler.TrxId, table *access.Table, innerExecutor Executor) *Delete {
	return &Delete{
		trxId:         trxId,
		table:         table,
		innerExecutor: innerExecutor,
	}
}

func (del *Delete) Next() (Record, error) {
	h := handler.Get()

	// 削除対象のレコードを先にすべて取得する
	// (削除により Iterator が参照するページデータが破壊されるのを防ぐ)
	var records []Record
	for {
		record, err := del.innerExecutor.Next()
		if err != nil {
			return nil, err
		}
		if record == nil {
			break
		}
		records = append(records, record)
	}

	// テーブルの FK 制約を確認
	tableMeta, _ := h.Catalog.GetTableMetaByName(del.table.Name)
	refFKs := h.Catalog.GetForeignKeysReferencingTable(del.table.Name)

	// 取得したレコードを削除
	for _, record := range records {
		// 排他ロックを先に取得してから FK チェックを行う
		// (ロック未保持の状態で FK チェックすると、並行する INSERT との競合で子行が孤立する可能性がある)
		if len(refFKs) > 0 {
			encodedKey := del.table.EncodeKey(record)
			btr := btree.NewBTree(del.table.MetaPageId)
			_, pos, err := btr.FindByKey(h.BufferPool, encodedKey)
			if err != nil {
				return nil, err
			}
			if err := h.LockMgr.Lock(del.trxId, pos, lock.Exclusive); err != nil {
				return nil, err
			}

			// FK チェック: 参照元テーブルからの参照がないことを確認
			if err := checkFKOnDelete(h.BufferPool, tableMeta, refFKs, record); err != nil {
				return nil, err
			}
		}

		if err := del.table.SoftDelete(h.BufferPool, del.trxId, h.LockMgr, record); err != nil {
			return nil, err
		}
	}

	return nil, nil
}
