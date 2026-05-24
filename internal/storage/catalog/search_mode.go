package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

var (
	_ SearchMode = (*SearchModeKey)(nil)
	_ SearchMode = (*SearchModeStart)(nil)
)

type SearchMode interface {
	Encode() btree.SearchMode
}
