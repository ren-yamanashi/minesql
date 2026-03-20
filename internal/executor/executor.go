package executor

type Executor interface {
	isExecutor()
}

type executor struct{}

func (executor) isExecutor() {}

// RecordIterator は行を逐次返す executor (SELECT 系)
type RecordIterator interface {
	Executor
	// 次の Record を取得する
	//
	// データがない場合、継続条件を満たさない場合は (nil, nil) を返す
	Next() (Record, error)
}

// FetchAll は RecordIterator から全レコードを取得する
func FetchAll(iter RecordIterator) ([]Record, error) {
	var results []Record
	for {
		record, err := iter.Next()
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

// Mutator は副作用を実行する executor (INSERT/DELETE/UPDATE/CREATE TABLE)
type Mutator interface {
	Executor
	Execute() error
}
