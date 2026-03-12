package executor

import "minesql/internal/storage"

type Delete struct {
	tableName     string
	InnerExecutor Executor
}

func NewDelete(tableName string, innerExecutor Executor) *Delete {
	return &Delete{
		tableName:     tableName,
		InnerExecutor: innerExecutor,
	}
}

func (del *Delete) Next() (Record, error) {
	sm := storage.GetStorageManager()

	tblMeta, err := sm.Catalog.GetTableMetadataByName(del.tableName)
	if err != nil {
		return nil, err
	}

	tbl, err := tblMeta.GetTable()
	if err != nil {
		return nil, err
	}

	// 削除対象のレコードを先にすべて収集する
	// (削除により Iterator が参照するページデータが破壊されるのを防ぐ)
	var records []Record
	for {
		record, err := del.InnerExecutor.Next()
		if err != nil {
			return nil, err
		}
		if record == nil {
			break
		}
		records = append(records, record)
	}

	// 収集したレコードを削除
	for _, record := range records {
		err = tbl.Delete(sm.BufferPoolManager, record)
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}
