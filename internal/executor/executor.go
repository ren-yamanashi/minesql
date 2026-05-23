package executor

import "github.com/ren-yamanashi/minesql/internal/storage/access"

type Executor interface {
	Next() (access.Record, error)
}
