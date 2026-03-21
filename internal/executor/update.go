package executor

import "minesql/internal/engine"

type SetColumn struct {
	Pos   uint16 // 更新対象のカラムの位置 (インデックス)
	Value []byte // 更新後の値
}

type Update struct {
	tableName     string
	SetColumns    []SetColumn
	InnerExecutor Executor
}

func NewUpdate(tableName string, setColumns []SetColumn, innerExecutor Executor) *Update {
	return &Update{
		tableName:     tableName,
		SetColumns:    setColumns,
		InnerExecutor: innerExecutor,
	}
}

func (upd *Update) Next() (Record, error) {
	sm := engine.Get()

	tblMeta, err := sm.Catalog.GetTableMetadataByName(upd.tableName)
	if err != nil {
		return nil, err
	}

	tbl, err := tblMeta.GetTable()
	if err != nil {
		return nil, err
	}

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
		err = tbl.Update(sm.BufferPool, record, updatedRecords[i])
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}
