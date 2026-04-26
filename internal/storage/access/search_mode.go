package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
)

// RecordSearchMode は検索方法を表すインターフェース
type RecordSearchMode interface {
	encode() btree.SearchMode
}

// RecordSearchModeStart は先頭から検索する
type RecordSearchModeStart struct{}

func (RecordSearchModeStart) encode() btree.SearchMode {
	return btree.SearchModeStart{}
}

// RecordSearchModeKey は指定したキーから検索する
type RecordSearchModeKey struct {
	Key [][]byte
}

func (k RecordSearchModeKey) encode() btree.SearchMode {
	var key []byte
	encode.Encode(k.Key, &key)
	return btree.SearchModeKey{Key: key}
}
