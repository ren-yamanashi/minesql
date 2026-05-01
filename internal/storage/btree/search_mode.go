package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type SearchMode interface {
	slotNum(ln *node.LeafNode) int
	childPageId(bn *node.BranchNode) (page.PageId, error)
}
