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

	// カラム名の順番と数が一致するか確認
	for i, colName := range ins.colNames {
		if tblMeta.Cols[i].Name != colName {
			return fmt.Errorf("column name does not match: expected %s, got %s", tblMeta.Cols[i].Name, colName)
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
