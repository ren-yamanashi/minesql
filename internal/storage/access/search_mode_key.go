package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
)

type SearchModeKey struct {
	Key [][]byte
}

func (k SearchModeKey) encode() btree.SearchMode {
	var key []byte
	encode.Encode(k.Key, &key)
	return btree.SearchModeKey{Key: key}
}
