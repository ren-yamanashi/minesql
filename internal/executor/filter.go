package executor

// Filter は InnerExecutor の結果から条件に合う行だけを返す
type Filter struct {
	whileCondition func(Record) bool
	innerExecutor  Executor
}

func NewFilter(innerExecutor Executor, condition func(Record) bool) *Filter {
	return &Filter{
		innerExecutor:  innerExecutor,
		whileCondition: condition,
	}
}

func (f *Filter) Next() (Record, error) {
	// 条件を満たすレコードを探す
	for {
		record, err := f.innerExecutor.Next()
		if err != nil {
			return nil, err
		}

		// データがなくなったら終了
		if record == nil {
			return nil, nil
		}

		// 条件を満たす場合はレコードを返す
		if f.whileCondition(record) {
			return record, nil
		}
	}
}
