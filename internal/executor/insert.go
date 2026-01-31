package executor

import (
	"minesql/internal/storage"
)

type Insert struct {
	tableName string
	colNames  []string
	records   [][][]byte
}

func NewInsert(tableName string, colNames []string, records [][][]byte) *Insert {
	return &Insert{
		tableName: tableName,
		colNames:  colNames,
		records:   records,
	}
}

func (ins *Insert) Next() (Record, error) {
	err := ins.execute(ins.records)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (ins *Insert) execute(records [][][]byte) error {
	sm := storage.GetStorageManager()

	tblMeta, err := sm.Catalog.GetTableMetadataByName(ins.tableName)
	if err != nil {
		return err
	}

	tbl, err := tblMeta.GetTable()
	if err != nil {
		return err
	}

	for _, record := range records {
		err := tbl.Insert(sm.BufferPoolManager, record)
		if err != nil {
			return err
		}
	}
	return nil
}
