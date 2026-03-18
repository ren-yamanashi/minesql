package btree

import (
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/page"
)

type SearchMode interface {
	childPageId(bn *node.BranchNode) page.PageId
	slotNum(ln *node.LeafNode) int
}

// 先頭から検索
type SearchModeStart struct{}

// 先頭の子ページIDを取得
func (sm SearchModeStart) childPageId(bn *node.BranchNode) page.PageId {
	return bn.ChildPageIdAt(0)
}

// 先頭のスロット番号を取得
func (sm SearchModeStart) slotNum(ln *node.LeafNode) int {
	return 0
}

// 指定したキーから検索
type SearchModeKey struct {
	Key []byte
}

// 指定したキーに基づいて子ページIDを取得
func (sm SearchModeKey) childPageId(bn *node.BranchNode) page.PageId {
	childIndex := bn.SearchChildSlotNum(sm.Key)
	return bn.ChildPageIdAt(childIndex)
}

// 指定したキーに基づいてスロット番号を取得
func (sm SearchModeKey) slotNum(ln *node.LeafNode) int {
	slotNum, _ := ln.SearchSlotNum(sm.Key)
	return slotNum
}
