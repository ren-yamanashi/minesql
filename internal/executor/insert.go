package executor

import (
	"minesql/internal/engine"
)

type Insert struct {
	tableName string
	colNames  []string
	records   []Record
}

func NewInsert(tableName string, colNames []string, records []Record) *Insert {
	return &Insert{
		tableName: tableName,
		colNames:  colNames,
		records:   records,
	}
}

// Insert は iterable ではないので、Next() は一度だけ呼び出される
//
// 戻り値も (エラー以外は) 常に (nil, nil) を返す
func (ins *Insert) Next() (Record, error) {
	sm := engine.Get()

	tblMeta, err := sm.Catalog.GetTableMetadataByName(ins.tableName)
	if err != nil {
		return nil, err
	}

	tbl, err := tblMeta.GetTable()
	if err != nil {
		return nil, err
	}

	for _, record := range ins.records {
		err := tbl.Insert(sm.BufferPool, record)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}
