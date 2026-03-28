package executor

import (
	"bytes"
	"minesql/internal/access"
	"minesql/internal/engine"
)

type SetColumn struct {
	Pos   uint16 // 更新対象のカラムの位置 (インデックス)
	Value []byte // 更新後の値
}

// Update は InnerExecutor の結果を元にレコードを更新する
type Update struct {
	table         *access.TableAccessMethod
	SetColumns    []SetColumn
	InnerExecutor Executor
}

func NewUpdate(table *access.TableAccessMethod, setColumns []SetColumn, innerExecutor Executor) *Update {
	return &Update{
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
			if err := upd.table.UpdateInplace(e.BufferPool, record, updatedRecords[i]); err != nil {
				return nil, err
			}
		} else {
			// プライマリキーが変わる場合はソフトデリート + Insert
			if err := upd.table.SoftDelete(e.BufferPool, record); err != nil {
				return nil, err
			}
			if err := upd.table.Insert(e.BufferPool, updatedRecords[i]); err != nil {
				return nil, err
			}
		}
	}

	return nil, nil
}
