package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

var (
	_ SearchMode = SearchModeKey{}
	_ SearchMode = SearchModeStart{}
)

type SearchMode interface {
	slotNum(ln *leafNode) int
	childPageId(bn *branchNode) (page.Id, error)
}
