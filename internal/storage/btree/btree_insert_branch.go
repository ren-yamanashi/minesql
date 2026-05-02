package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// insertBranchOverflow は子ノードからのオーバーフローレコードをブランチノードに挿入する
//   - branchNode: 挿入先のブランチノード
//   - childSlotNum: 子ノードのスロット番号
//   - overflowKey: 子ノードから伝播された境界キー
//   - overflowChildPageId: 子ノードの分割で作られたページの PageId
//   - return:
//   - overflow: 分割時の境界キー (分割なしの場合は nil)
//   - newPageId: 分割で作られたブランチノードの PageId (分割なしの場合は InvalidPageId)
func (bt *Btree) insertBranchOverflow(
	branchNode *node.BranchNode,
	childSlotNum int,
	overflowKey []byte,
	overflowChildPageId page.PageId,
) (overflow []byte, newPageId page.PageId, err error) {
	overflowRecord := node.NewRecord([]byte{}, overflowKey, overflowChildPageId.ToBytes())

	// ブランチノードに挿入できた場合は終了
	if branchNode.Insert(childSlotNum, overflowRecord) {
		return nil, page.InvalidPageId, nil
	}

	// ブランチノードが満杯の場合は分割
	return bt.splitInsertBranch(branchNode, overflowRecord)
}

// splitInsertBranch はブランチノードを分割してレコードを挿入する
//   - branchNode: 分割元のブランチノード
//   - record: 挿入するレコード
//   - return: 境界キー, 新しいブランチノードの PageId
func (bt *Btree) splitInsertBranch(
	branchNode *node.BranchNode,
	record node.Record,
) ([]byte, page.PageId, error) {
	newBranchPageId, err := bt.bufferPool.AllocatePageId(bt.MetaPageId.FileId)
	if err != nil {
		return nil, page.InvalidPageId, err
	}
	_, err = bt.bufferPool.AddPage(newBranchPageId)
	if err != nil {
		return nil, page.InvalidPageId, err
	}
	defer bt.bufferPool.UnRefPage(newBranchPageId)

	pageNewBranch, err := bt.bufferPool.GetWritePage(newBranchPageId)
	if err != nil {
		return nil, page.InvalidPageId, err
	}
	newBranch := node.NewBranchNode(pageNewBranch)
	overflowKey, err := branchNode.SplitInsert(newBranch, record)
	if err != nil {
		return nil, page.InvalidPageId, err
	}

	return overflowKey, newBranchPageId, nil
}
