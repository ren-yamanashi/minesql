package executor

import (
	"errors"
	"fmt"
	"minesql/internal/storage"
)

type Insert struct {
	tableName string
	colNames  []string
	records   [][][]byte
}

func NewInsert(tableName string, colNames []string, records [][][]byte) (*Insert, error) {
	if len(colNames) == 0 {
		return nil, errors.New("column names cannot be empty")
	}
	if len(records) == 0 {
		return nil, errors.New("records cannot be empty")
	}
	return &Insert{
		tableName: tableName,
		colNames:  colNames,
		records:   records,
	}, nil
}

func (ins *Insert) Next() (Record, error) {
	err := ins.execute(ins.records)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (ins *Insert) execute(records [][][]byte) error {
	engine := storage.GetStorageManager()
	bpm := engine.GetBufferPoolManager()
	cat := engine.GetCatalog()

	tblMeta, err := cat.GetTableMetadataByName(ins.tableName)
	if err != nil {
		return err
	}

	// カラム名の順番と数が一致するか確認
	for i, colName := range ins.colNames {
		if tblMeta.Cols[i].Name != colName {
			return fmt.Errorf("column name does not match: expected %s, got %s", tblMeta.Cols[i].Name, colName)
		}
	}

	tbl, err := engine.GetTable(ins.tableName)
	if err != nil {
		return err
	}

	for _, record := range records {
		err := tbl.Insert(bpm, record)
		if err != nil {
			return err
		}
	}
	return nil
}
