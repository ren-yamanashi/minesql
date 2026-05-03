package access

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type SearchMode interface {
	encode() btree.SearchMode
}
