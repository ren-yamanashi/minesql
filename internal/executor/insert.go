package executor

import (
	"minesql/internal/storage"
)

type Insert struct {
	tableName string
	records   [][][]byte
}

func NewInsert(tableName string, records [][][]byte) *Insert {
	return &Insert{
		tableName: tableName,
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
	engine := storage.GetStorageManager()
	bpm := engine.GetBufferPoolManager()

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
