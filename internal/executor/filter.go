package executor

// Filter は InnerExecutor の結果から条件に合う行だけを返す
type Filter struct {
	InnerExecutor Executor
	condition     func(Record) bool
}

func NewFilter(innerExecutor Executor, condition func(Record) bool) *Filter {
	return &Filter{
		InnerExecutor: innerExecutor,
		condition:     condition,
	}
}

func (f *Filter) Next() (Record, error) {
	// 条件を満たすレコードを探す
	for {
		record, err := f.InnerExecutor.Next()
		if err != nil {
			return nil, err
		}

		// データがなくなったら終了
		if record == nil {
			return nil, nil
		}

		// 条件を満たす場合はレコードを返す
		if f.condition(record) {
			return record, nil
		}
	}
}
