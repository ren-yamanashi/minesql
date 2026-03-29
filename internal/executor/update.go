package executor

import (
	"bytes"
	"minesql/internal/access"
	"minesql/internal/engine"
	"minesql/internal/undo"
)

type SetColumn struct {
	Pos   uint16 // 更新対象のカラムの位置 (インデックス)
	Value []byte // 更新後の値
}

// Update は InnerExecutor の結果を元にレコードを更新する
type Update struct {
	trx           *Transaction
	table         *access.TableAccessMethod
	SetColumns    []SetColumn
	InnerExecutor Executor
}

func NewUpdate(trx *Transaction, table *access.TableAccessMethod, setColumns []SetColumn, innerExecutor Executor) *Update {
	return &Update{
		trx:           trx,
		table:         table,
		SetColumns:    setColumns,
		InnerExecutor: innerExecutor,
	}
}

func (upd *Update) Next() (Record, error) {
	e := engine.Get()

	// 更新対象のレコードを先にすべて収集する
	// (更新により Iterator が参照するページデータが破壊されるのを防ぐ)
	var records []Record
	for {
		record, err := upd.InnerExecutor.Next()
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

		for _, setCol := range upd.SetColumns {
			updatedRecord[setCol.Pos] = setCol.Value
		}
		updatedRecords = append(updatedRecords, updatedRecord)
	}

	// 更新後のレコードで更新を実行
	for i, record := range records {
		oldRec := access.NewRecord(record, upd.table.PrimaryKeyCount)
		newRec := access.NewRecord(updatedRecords[i], upd.table.PrimaryKeyCount)
		encodedOldKey := oldRec.EncodeKey()
		encodedNewKey := newRec.EncodeKey()

		if bytes.Equal(encodedOldKey, encodedNewKey) {
			// プライマリキーが変わらない場合はインプレース更新
			upd.trx.AddUndoLogRecord(undo.NewUpdateInplaceLogRecord(upd.table, record, updatedRecords[i]))
			if err := upd.table.UpdateInplace(e.BufferPool, record, updatedRecords[i]); err != nil {
				return nil, err
			}
		} else {
			// プライマリキーが変わる場合はソフトデリート + Insert
			upd.trx.AddUndoLogRecord(undo.NewDeleteLogRecord(upd.table, record))
			if err := upd.table.SoftDelete(e.BufferPool, record); err != nil {
				return nil, err
			}
			upd.trx.AddUndoLogRecord(undo.NewInsertLogRecord(upd.table, updatedRecords[i]))
			if err := upd.table.Insert(e.BufferPool, updatedRecords[i]); err != nil {
				return nil, err
			}
		}
	}

	return nil, nil
}
