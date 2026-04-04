package executor

import (
	"bytes"
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
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

	// 更新後のレコードで更新を実行
	for i, record := range records {
		encodedOldKey := upd.table.EncodeKey(record)
		encodedNewKey := upd.table.EncodeKey(updatedRecords[i])

		if bytes.Equal(encodedOldKey, encodedNewKey) {
			// プライマリキーが変わらない場合はインプレース更新
			hdl.AppendUpdateInplaceUndo(upd.trxId, upd.table, record, updatedRecords[i])
			if err := upd.table.UpdateInplace(hdl.BufferPool, upd.trxId, hdl.LockMgr, record, updatedRecords[i]); err != nil {
				return nil, err
			}
		} else {
			// プライマリキーが変わる場合はソフトデリート + Insert
			hdl.AppendDeleteUndo(upd.trxId, upd.table, record)
			if err := upd.table.SoftDelete(hdl.BufferPool, upd.trxId, hdl.LockMgr, record); err != nil {
				return nil, err
			}
			hdl.AppendInsertUndo(upd.trxId, upd.table, updatedRecords[i])
			if err := upd.table.Insert(hdl.BufferPool, updatedRecords[i]); err != nil {
				return nil, err
			}
		}
	}

	return nil, nil
}
