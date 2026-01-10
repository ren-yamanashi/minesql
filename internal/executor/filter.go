package executor

import "minesql/internal/storage/bufferpool"

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

// 次の Record を取得する
// データがない場合、継続条件を満たさない場合は (nil, nil) を返す
func (f *Filter) Next(bpm *bufferpool.BufferPoolManager) (Record, error) {
	// 条件を満たすレコードを探す
	for {
		record, err := f.InnerExecutor.Next(bpm)
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
