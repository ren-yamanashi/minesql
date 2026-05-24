package executor

import "github.com/ren-yamanashi/minesql/internal/storage/access"

// RowIterator は行を 1 行ずつ返すイテレータ
type RowIterator interface {
	Next() (access.Record, bool, error)
}

// Executor はクエリを実行し、影響行数を返す
type Executor interface {
	Execute() (int, error)
}
