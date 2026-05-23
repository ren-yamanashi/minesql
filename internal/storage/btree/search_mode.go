package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type SearchMode interface {
	slotNum(ln *leafNode) int
	childPageId(bn *branchNode) (page.Id, error)
}
