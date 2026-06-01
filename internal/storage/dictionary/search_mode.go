package dictionary

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

var (
	_ SearchMode = SearchModeKey{}
	_ SearchMode = SearchModeStart{}
)

type SearchMode interface {
	Encode() btree.SearchMode
}
