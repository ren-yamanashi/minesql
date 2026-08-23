package btree

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// insertBranchOverflow は子ノードからのオーバーフローレコードをブランチノードに挿入する
//   - branchNode: 挿入先のブランチノード
//   - childSlotNum: 子ノードのスロット番号
//   - overflowKey: 子ノードから伝播された境界キー
//   - overflowChildPageId: 子ノードの分割で作られたページの PageId
//   - reserved: 事前確保済みのページ集合 (分割時に branch ページを取り出す)
//   - return:
//   - overflow: 分割時の境界キー (分割なしの場合は nil)
//   - newPageId: 分割で作られたブランチノードの PageId (分割なしの場合は InvalidPageId)
func (t *Tree) insertBranchOverflow(
	mtr *buffer.Mtr,
	branchNode *branchNode,
	childSlotNum int,
	overflowKey []byte,
	overflowChildPageId page.Id,
	reserved *reservedPages,
) (overflow []byte, newPageId page.Id) {
	overflowRecord := NewRecord([]byte{}, overflowKey, overflowChildPageId.Bytes())

	// ブランチノードに挿入できた場合は終了
	if branchNode.insert(childSlotNum, overflowRecord) {
		return nil, page.InvalidId()
	}

	// ブランチノードが満杯の場合は分割
	return t.splitInsertBranch(mtr, branchNode, overflowRecord, reserved)
}

// splitInsertBranch はブランチノードを分割してレコードを挿入する
//   - branchNode: 分割元のブランチノード
//   - record: 挿入するレコード
//   - reserved: 事前確保済みのページ集合 (branch ページを 1 枚取り出す)
//   - return: 境界キー, 新しいブランチノードの PageId
func (t *Tree) splitInsertBranch(
	mtr *buffer.Mtr,
	branchNode *branchNode,
	record Record,
	reserved *reservedPages,
) ([]byte, page.Id) {
	newBranchPageId := reserved.takeBranch()
	pageNewBranch, err := mtr.PageForWrite(newBranchPageId)
	if err != nil {
		panic(fmt.Sprintf("btree: PageForWrite failed for reserved branch page (pageId=%v): %v", newBranchPageId, err))
	}
	newBranch := newBranchNode(pageNewBranch)
	overflowKey := branchNode.splitInsert(newBranch, record)

	return overflowKey, newBranchPageId
}
