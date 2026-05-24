package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type SearchModeStart struct{}

func (SearchModeStart) Encode() btree.SearchMode {
	return btree.SearchModeStart{}
}
