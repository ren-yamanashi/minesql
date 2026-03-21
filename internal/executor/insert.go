package executor

import (
	"minesql/internal/access"
	"minesql/internal/engine"
)

// Insert はレコードを追加する
type Insert struct {
	table    *access.TableAccessMethod
	colNames []string
	records  []Record
}

func NewInsert(table *access.TableAccessMethod, colNames []string, records []Record) *Insert {
	return &Insert{
		table:    table,
		colNames: colNames,
		records:  records,
	}
}

func (ins *Insert) Next() (Record, error) {
	e := engine.Get()

	for _, record := range ins.records {
		err := ins.table.Insert(e.BufferPool, record)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}
