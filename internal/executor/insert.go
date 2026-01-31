package executor

import (
	"fmt"
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

	// カラム名が一致するか検証
	for _, colName := range ins.colNames {
		_, found := tblMeta.GetColByName(colName)
		if !found {
			return fmt.Errorf("column name does not match: column %s not found in table %s", colName, ins.tableName)
		}
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
