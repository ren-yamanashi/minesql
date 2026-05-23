package btree

import (
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
	branchNode *BranchNode,
	childSlotNum int,
	overflowKey []byte,
	overflowChildPageId page.Id,
) (overflow []byte, newPageId page.Id, err error) {
	overflowRecord := NewRecord([]byte{}, overflowKey, overflowChildPageId.ToBytes())

	// ブランチノードに挿入できた場合は終了
	if branchNode.Insert(childSlotNum, overflowRecord) {
		return nil, page.InvalidId, nil
	}

	// ブランチノードが満杯の場合は分割
	return bt.splitInsertBranch(branchNode, overflowRecord)
}

// splitInsertBranch はブランチノードを分割してレコードを挿入する
//   - branchNode: 分割元のブランチノード
//   - record: 挿入するレコード
//   - return: 境界キー, 新しいブランチノードの PageId
func (bt *Btree) splitInsertBranch(
	branchNode *BranchNode,
	record Record,
) ([]byte, page.Id, error) {
	newBranchPageId, err := bt.bufferPool.AllocatePageId(bt.MetaPageId.FileId)
	if err != nil {
		return nil, page.InvalidId, err
	}
	_, err = bt.bufferPool.AddPage(newBranchPageId)
	if err != nil {
		return nil, page.InvalidId, err
	}
	defer bt.bufferPool.UnRefPage(newBranchPageId)

	pageNewBranch, err := bt.bufferPool.BufferPageForWrite(newBranchPageId)
	if err != nil {
		return nil, page.InvalidId, err
	}
	newBranch := NewBranchNode(pageNewBranch.Page)
	overflowKey, err := branchNode.SplitInsert(newBranch, record)
	if err != nil {
		return nil, page.InvalidId, err
	}

	return overflowKey, newBranchPageId, nil
}
