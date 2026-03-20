package executor

import "minesql/internal/engine"

type Delete struct {
	executor
	tableName string
	Iterator  RecordIterator
}

func NewDelete(tableName string, iterator RecordIterator) *Delete {
	return &Delete{
		tableName: tableName,
		Iterator:  iterator,
	}
}

// Execute は Iterator を使用して検索条件に一致したレコードを削除する
func (del *Delete) Execute() error {
	sm := engine.Get()

	tblMeta, err := sm.Catalog.GetTableMetadataByName(del.tableName)
	if err != nil {
		return err
	}

	tbl, err := tblMeta.GetTable()
	if err != nil {
		return err
	}

	// 削除対象のレコードを先にすべて取得する
	// (削除により Iterator が参照するページデータが破壊されるのを防ぐ)
	var records []Record
	for {
		record, err := del.Iterator.Next()
		if err != nil {
			return err
		}
		if record == nil {
			break
		}
		records = append(records, record)
	}

	// 取得したレコードを削除
	for _, record := range records {
		err = tbl.Delete(sm.BufferPool, record)
		if err != nil {
			return err
		}
	}

	return nil
}
