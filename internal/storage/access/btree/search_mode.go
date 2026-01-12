package btree

import (
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/page"
)

type SearchMode interface {
	childPageId(bn *node.BranchNode) page.PageId
}

// =======================
// 先頭から検索
// =======================
type SearchModeStart struct{}

// 先頭の子ページIDを取得
func (sm SearchModeStart) childPageId(bn *node.BranchNode) page.PageId {
	return bn.ChildPageIdAt(0)
}

// =======================
// 指定したキーから検索
// =======================
type SearchModeKey struct {
	Key []byte
}

// 指定したキーに基づいて子ページIDを取得
func (sm SearchModeKey) childPageId(bn *node.BranchNode) page.PageId {
	return bn.SearchChildPageId(sm.Key)
}
