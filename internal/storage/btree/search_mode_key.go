package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// SearchModeKey は指定したキーから検索するモード
type SearchModeKey struct {
	Key []byte
}

// slotNum は指定した key に基づいてスロット番号を取得
func (sm SearchModeKey) slotNum(ln *node.LeafNode) int {
	slotNum, _ := ln.SearchSlotNum(sm.Key)
	return slotNum
}

// childPageId は指定した key に基づいて子の PageId を取得する
func (sm SearchModeKey) childPageId(bn *node.BranchNode) (page.PageId, error) {
	slotNum, found := bn.SearchSlotNum(sm.Key)
	if found {
		slotNum++ // 境界キーと一致する場合、右の子に属する
	}
	return bn.ChildPageId(slotNum)
}
