package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
)

type SearchModeKey struct {
	Key [][]byte
}

func (k SearchModeKey) Encode() btree.SearchMode {
	key := encode.Encode(nil, k.Key)
	return btree.SearchModeKey{Key: key}
}
