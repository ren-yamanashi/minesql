package evict

import "minesql/internal/storage/bufferpool/buftype"

// EvictAlgorithm はバッファプールからページを追い出すアルゴリズム (インターフェース)
type EvictAlgorithm interface {
	Access(bufferId buftype.BufferId)
	Evict() buftype.BufferId
	Remove(bufferId buftype.BufferId)
}
