package executor

type Filter struct {
	executor
	InnerIterator RecordIterator // Filter 内部で使用する RecordIterator (この RecordIterator から取得したレコードに対してフィルタを適用する)
	condition     func(Record) bool
}

func NewFilter(innerIterator RecordIterator, condition func(Record) bool) *Filter {
	return &Filter{
		InnerIterator: innerIterator,
		condition:     condition,
	}
}

func (f *Filter) Next() (Record, error) {
	// 条件を満たすレコードを探す
	for {
		record, err := f.InnerIterator.Next()
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
