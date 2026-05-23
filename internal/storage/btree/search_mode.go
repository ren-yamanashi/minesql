package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type SearchMode interface {
	slotNum(ln *LeafNode) int
	childPageId(bn *BranchNode) (page.PageId, error)
}
