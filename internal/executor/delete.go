package executor

import "minesql/internal/engine"

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
	sm := engine.Get()

	tblMeta, err := sm.Catalog.GetTableMetadataByName(del.tableName)
	if err != nil {
		return nil, err
	}

	tbl, err := tblMeta.GetTable()
	if err != nil {
		return nil, err
	}

	// 削除対象のレコードを先にすべて取得する
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

	// 取得したレコードを削除
	for _, record := range records {
		err = tbl.Delete(sm.BufferPool, record)
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}
