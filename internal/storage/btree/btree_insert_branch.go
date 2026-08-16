package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
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
func (t *Tree) insertBranchOverflow(
	mtr *buffer.Mtr,
	branchNode *branchNode,
	childSlotNum int,
	overflowKey []byte,
	overflowChildPageId page.Id,
) (overflow []byte, newPageId page.Id, err error) {
	overflowRecord := NewRecord([]byte{}, overflowKey, overflowChildPageId.Bytes())

	// ブランチノードに挿入できた場合は終了
	if branchNode.insert(childSlotNum, overflowRecord) {
		return nil, page.InvalidId(), nil
	}

	// ブランチノードが満杯の場合は分割
	return t.splitInsertBranch(mtr, branchNode, overflowRecord)
}

// splitInsertBranch はブランチノードを分割してレコードを挿入する
//   - branchNode: 分割元のブランチノード
//   - record: 挿入するレコード
//   - return: 境界キー, 新しいブランチノードの PageId
func (t *Tree) splitInsertBranch(
	mtr *buffer.Mtr,
	branchNode *branchNode,
	record Record,
) ([]byte, page.Id, error) {
	newBranchPageId, err := fsp.AllocatePage(mtr, t.MetaPageId().FileId())
	if err != nil {
		return nil, page.InvalidId(), err
	}
	_, err = t.bufferPool.AddPage(newBranchPageId)
	if err != nil {
		return nil, page.InvalidId(), err
	}

	pageNewBranch, err := mtr.PageForWrite(newBranchPageId)
	if err != nil {
		return nil, page.InvalidId(), err
	}
	newBranch := newBranchNode(pageNewBranch)
	overflowKey, err := branchNode.splitInsert(newBranch, record)
	if err != nil {
		return nil, page.InvalidId(), err
	}

	return overflowKey, newBranchPageId, nil
}
