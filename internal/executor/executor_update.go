package executor

import (
	"bytes"

	"github.com/ren-yamanashi/minesql/internal/storage/access"
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/handler"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

type SetColumn struct {
	Pos   uint16 // 更新対象のカラムの位置 (インデックス)
	Value []byte // 更新後の値
}

// Update は InnerExecutor の結果を元にレコードを更新する
type Update struct {
	trxId         handler.TrxId
	table         *access.Table
	setColumns    []SetColumn
	innerExecutor Executor
}

func NewUpdate(trxId handler.TrxId, table *access.Table, setColumns []SetColumn, innerExecutor Executor) *Update {
	return &Update{
		trxId:         trxId,
		table:         table,
		setColumns:    setColumns,
		innerExecutor: innerExecutor,
	}
}

func (upd *Update) Next() (Record, error) {
	hdl := handler.Get()

	// 更新対象のレコードを先にすべて収集する
	// (更新により Iterator が参照するページデータが破壊されるのを防ぐ)
	var records []Record
	for {
		record, err := upd.innerExecutor.Next()
		if err != nil {
			return nil, err
		}
		if record == nil {
			break
		}
		records = append(records, record)
	}

	// 更新後のレコードを作成
	var updatedRecords []Record
	for _, record := range records {
		updatedRecord := make(Record, len(record))
		copy(updatedRecord, record)

		for _, setCol := range upd.setColumns {
			updatedRecord[setCol.Pos] = setCol.Value
		}
		updatedRecords = append(updatedRecords, updatedRecord)
	}

	// テーブルの FK 制約を確認
	tableMeta, _ := hdl.Catalog.GetTableMetaByName(upd.table.Name)
	fks := tableMeta.GetForeignKeyConstraints()
	refFKs := hdl.Catalog.GetForeignKeysReferencingTable(upd.table.Name)
	hasFKChecks := len(fks) > 0 || len(refFKs) > 0

	// 更新後のレコードで更新を実行
	for i, record := range records {
		encodedOldKey := upd.table.EncodeKey(record)
		encodedNewKey := upd.table.EncodeKey(updatedRecords[i])

		// FK チェック: 排他ロックを先に取得してから FK 制約を検証する
		if hasFKChecks {
			btr := btree.NewBTree(upd.table.MetaPageId)
			_, pos, err := btr.FindByKey(hdl.BufferPool, encodedOldKey)
			if err != nil {
				return nil, err
			}
			if err := hdl.LockMgr.Lock(upd.trxId, pos, lock.Exclusive); err != nil {
				return nil, err
			}

			if err := checkFKOnUpdate(hdl.BufferPool, upd.trxId, hdl.LockMgr, tableMeta, refFKs, fks, record, updatedRecords[i]); err != nil {
				return nil, err
			}
		}

		if bytes.Equal(encodedOldKey, encodedNewKey) {
			// プライマリキーが変わらない場合はインプレース更新
			if err := upd.table.UpdateInplace(hdl.BufferPool, upd.trxId, hdl.LockMgr, record, updatedRecords[i]); err != nil {
				return nil, err
			}
		} else {
			// プライマリキーが変わる場合はソフトデリート + Insert
			if err := upd.table.SoftDelete(hdl.BufferPool, upd.trxId, hdl.LockMgr, record); err != nil {
				return nil, err
			}
			if err := upd.table.Insert(hdl.BufferPool, upd.trxId, hdl.LockMgr, updatedRecords[i]); err != nil {
				return nil, err
			}
		}
	}

	return nil, nil
}
