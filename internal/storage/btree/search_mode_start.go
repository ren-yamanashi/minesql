package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// SearchModeStart は先頭から検索するモード
type SearchModeStart struct{}

// slotNum は先頭のスロット番号を取得する
func (sm SearchModeStart) slotNum(ln *LeafNode) int { return 0 }

// childPageId は先頭の子の PageId を取得する
func (sm SearchModeStart) childPageId(bn *BranchNode) (page.Id, error) {
	return bn.ChildPageId(0)
}
