package executor

import (
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/table"
)

type RecordSearchMode interface {
	Encode() btree.SearchMode
}

// =======================
// 先頭から検索
// =======================
type RecordSearchModeStart struct{}

func (RecordSearchModeStart) Encode() btree.SearchMode {
	return btree.SearchModeStart{}
}

// =======================
// 指定したキーから検索
// =======================
type RecordSearchModeKey struct {
	Key [][]byte
}

func (k RecordSearchModeKey) Encode() btree.SearchMode {
	var key []byte
	table.Encode(k.Key, &key)
	return btree.SearchModeKey{Key: key}
}
