package executor

import "io"

type Executor interface {
	Next() (Record, error)
}

func ExecutePlan(executor Executor) ([]Record, error) {
	var results []Record
	for {
		record, err := executor.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if record == nil {
			break
		}
		results = append(results, record)
	}
	return results, nil
}
