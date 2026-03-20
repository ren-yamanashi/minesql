package executor

type Executor interface {
	// 次の Record を取得する
	//
	// データがない場合、継続条件を満たさない場合は (nil, nil) を返す
	Next() (Record, error)
}

func ExecutePlan(executor Executor) ([]Record, error) {
	var results []Record
	for {
		record, err := executor.Next()
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
