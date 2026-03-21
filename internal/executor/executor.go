package executor

type Record [][]byte

type Executor interface {
	// 次の Record を取得する
	//
	// データがない場合、継続条件を満たさない場合は (nil, nil) を返す
	Next() (Record, error)
}
