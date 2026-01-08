package btree

import (
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/disk"
)

type SearchMode interface {
	ChildPageId(bn *node.BranchNode) disk.PageId
}

// =======================
// 先頭から検索
// =======================
type SearchModeStart struct{}

// 先頭の子ページIDを取得
func (sm SearchModeStart) ChildPageId(bn *node.BranchNode) disk.PageId {
	return bn.ChildPageIdAt(0)
}

// =======================
// 指定したキーから検索
// =======================
type SearchModeKey struct {
	Key []byte
}

// 指定したキーに基づいて子ページIDを取得
func (sm SearchModeKey) ChildPageId(bn *node.BranchNode) disk.PageId {
	return bn.SearchChildPageId(sm.Key)
}
