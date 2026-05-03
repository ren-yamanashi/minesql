package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type SearchMode interface {
	encode() btree.SearchMode
}
