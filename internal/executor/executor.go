package executor

type Executor interface {
	Next() (Record, error)
}
