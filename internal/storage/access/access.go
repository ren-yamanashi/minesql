package access

import "minesql/internal/storage/bufferpool"

type AccessMethod interface {
	Create(bp *bufferpool.BufferPool) error
}
