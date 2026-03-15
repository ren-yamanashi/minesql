package evict

import "minesql/internal/storage/bufferpool/buftype"

// EvictAlgorithm はバッファプールからページを追い出すアルゴリズム (インターフェース)
type EvictAlgorithm interface {
	// Access はページがアクセスされたことを記録する
	Access(bufferId buftype.BufferId)
	// Evict は追い出すページの BufferId を返す
	Evict() buftype.BufferId
	// Remove はページの参照を解除し、優先的に追い出されるようにする
	Remove(bufferId buftype.BufferId)
}
