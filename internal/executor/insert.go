package executor

import "minesql/internal/storage"

type Insert struct {
	tableName string
}

func NewInsert(tableName string) *Insert {
	return &Insert{
		tableName: tableName,
	}
}

func (ins *Insert) Execute(values [][]byte) error {
	engine := storage.GetStorageEngine()
	bpm := engine.GetBufferPoolManager()

	tbl, err := engine.GetTable(ins.tableName)
	if err != nil {
		return err
	}

	return tbl.Insert(bpm, values)
}

// NOTE: Executor インターフェースを実装するためのダミー実装
func (ins *Insert) Next() (Record, error) {
	return nil, nil
}
