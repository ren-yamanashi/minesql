package executor

import "github.com/ren-yamanashi/minesql/internal/storage/access"

// RowIterator は行を 1 行ずつ返すイテレータ
type RowIterator interface {
	Next() (access.Record, bool, error)
}
