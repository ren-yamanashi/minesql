package executor

import (
	"io"
	"minesql/internal/storage"
)

type Insert struct {
	tableName string
	records   [][][]byte
	executed  bool
}

func NewInsert(tableName string, records [][][]byte) *Insert {
	return &Insert{
		tableName: tableName,
		records:   records,
		executed:  false,
	}
}

func (ins *Insert) Next() (Record, error) {
	if ins.executed {
		return nil, io.EOF
	}
	err := ins.execute(ins.records)
	if err != nil {
		return nil, err
	}
	ins.executed = true
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
