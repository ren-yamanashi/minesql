package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type SearchModeStart struct{}

func (SearchModeStart) encode() btree.SearchMode { return btree.SearchModeStart{} }
