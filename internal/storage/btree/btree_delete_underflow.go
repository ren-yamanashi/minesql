package btree

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// siblingInfo は兄弟ノードの情報を表す
type siblingInfo struct {
	pageId     page.Id
	bufferPage *buffer.Page
	isLeft     bool // true: 兄弟ノードは左の兄弟ノード, false: 兄弟ノードは右の兄弟ノード
}

// deleteUnderflow はアンダーフローした子ノードに対して転送またはマージを行う
//   - branchNode: 親ブランチノード
//   - childBufPage: アンダーフローが発生した子ノードのバッファページ
//   - childSlotNum: 子ノードのスロット番号
//   - metaPage: リーフマージ時に leafPageCount を減算するために引き渡す
//   - return: underflow (親ブランチノードがアンダーフローしたか)
func (t *Tree) deleteUnderflow(
	mtr *buffer.Mtr,
	branchNode *branchNode,
	childBufPage *buffer.Page,
	childSlotNum int,
	metaPage *metaPage,
) (underflow bool, err error) {
	sibling, err := t.findSibling(mtr, branchNode, childSlotNum)
	if err != nil {
		return false, err
	}
	childPage, err := mtr.PageForRead(childBufPage.PageId())
	if err != nil {
		return false, err
	}
	if nodeType(childPage.Data()) == nodeTypeLeaf {
		return t.onLeafUnderflow(mtr, branchNode, childBufPage, sibling, childSlotNum, metaPage)
	}
	return t.onBranchUnderflow(mtr, branchNode, childBufPage, sibling, childSlotNum)
}

// findSibling は転送・マージ対象の兄弟ノードを決定する
func (t *Tree) findSibling(mtr *buffer.Mtr, branchNode *branchNode, childSlotNum int) (siblingInfo, error) {
	if childSlotNum < branchNode.numRecords() {
		siblingPageId, err := branchNode.childPageId(childSlotNum + 1)
		if err != nil {
			return siblingInfo{}, err
		}
		bufPage, err := mtr.PageForRead(siblingPageId)
		if err != nil {
			return siblingInfo{}, err
		}
		return siblingInfo{pageId: siblingPageId, bufferPage: bufPage, isLeft: false}, nil
	}
	siblingPageId, err := branchNode.childPageId(childSlotNum - 1)
	if err != nil {
		return siblingInfo{}, err
	}
	bufPage, err := mtr.PageForRead(siblingPageId)
	if err != nil {
		return siblingInfo{}, err
	}
	return siblingInfo{pageId: siblingPageId, bufferPage: bufPage, isLeft: true}, nil
}

// onLeafUnderflow はリーフノードのアンダーフロー処理を行う (3 フェーズ)
//   - 判定 → touch → 書き込みの順に実行し、書き込みフェーズは失敗しない
func (t *Tree) onLeafUnderflow(
	mtr *buffer.Mtr,
	parentBranch *branchNode,
	childBufPage *buffer.Page,
	sibling siblingInfo,
	childSlotNum int,
	metaPage *metaPage,
) (underflow bool, err error) {
	pageChild, err := mtr.PageForWrite(childBufPage.PageId())
	if err != nil {
		return false, err
	}
	pageSibling, err := mtr.PageForWrite(sibling.pageId)
	if err != nil {
		return false, err
	}
	childLeaf := newLeafNode(pageChild)
	siblingLeaf := newLeafNode(pageSibling)

	// 判定フェーズ (読みのみ): 転送 / マージ / アンダーフロー許容
	if siblingLeaf.canTransferRecord(sibling.isLeft) && canUpdateParentForLeafTransfer(parentBranch, siblingLeaf, sibling.isLeft, childSlotNum) {
		t.transferLeaf(childLeaf, siblingLeaf, parentBranch, sibling.isLeft, childSlotNum)
		return false, nil
	}
	survivor, disappearing, survivorPageId, disappearingPageId := selectLeafMergePair(childLeaf, siblingLeaf, childBufPage.PageId(), sibling)
	if !survivor.canMergeAllFrom(disappearing) {
		return false, nil // ノードの容量を超えてマージ不可の場合はアンダーフローを許容する
	}
	if !canUpdateParentForRightMerge(parentBranch, sibling.isLeft, childSlotNum) {
		return false, nil // 親ブランチの境界キー差し替えが収まらない場合はアンダーフローを許容する
	}

	// touch フェーズ (書き込み開始前): 解放対象と relink 相手を全て取得する
	plan, err := fsp.TouchFreeSegmentPage(mtr, t.leafSegmentHeaderAt(), disappearingPageId)
	if err != nil {
		return false, err
	}
	if nextPageId := disappearing.nextPageId(); !nextPageId.IsInvalid() {
		if _, err := mtr.PageForWrite(nextPageId); err != nil {
			return false, err
		}
	}

	// 書き込みフェーズ: 失敗しない
	if !survivor.transferAllFrom(disappearing) {
		panic("btree: leaf merge transferAllFrom must succeed after canMergeAllFrom check")
	}
	t.relinkLeafAfterMerge(mtr, disappearing, survivor, survivorPageId)
	uf := t.updateParentForLeafMerge(parentBranch, sibling.isLeft, survivorPageId, childSlotNum)
	fsp.WriteFreeSegmentPage(mtr, plan)
	metaPage.setLeafPageCount(metaPage.leafPageCount() - 1)
	return uf, nil
}

// transferLeaf はリーフノード間の 1 レコード転送 (兄弟からの補充) を行う
func (t *Tree) transferLeaf(
	childLeaf, siblingLeaf *leafNode,
	parentBranch *branchNode,
	siblingIsLeft bool,
	childSlotNum int,
) {
	if siblingIsLeft {
		// 左の兄弟から転送: 左の兄弟の末尾を自分の先頭へ
		lastSlotNum := siblingLeaf.numRecords() - 1
		siblingRecord := siblingLeaf.record(lastSlotNum)
		if !childLeaf.insert(0, siblingRecord) {
			panic("btree: child leaf must have space for transfer from left sibling")
		}
		siblingLeaf.delete(lastSlotNum)
		parentRecord := parentBranch.record(childSlotNum - 1)
		updated := NewRecord(parentRecord.Header(), childLeaf.record(0).Key(), parentRecord.NonKey())
		if !parentBranch.update(childSlotNum-1, updated) {
			panic("btree: parent branch key update must succeed on leaf transfer")
		}
		return
	}
	// 右の兄弟から転送: 右の兄弟の先頭を末尾へ
	siblingRecord := siblingLeaf.record(0)
	if !childLeaf.insert(childLeaf.numRecords(), siblingRecord) {
		panic("btree: child leaf must have space for transfer from right sibling")
	}
	siblingLeaf.delete(0)
	parentRecord := parentBranch.record(childSlotNum)
	updated := NewRecord(parentRecord.Header(), siblingLeaf.record(0).Key(), parentRecord.NonKey())
	if !parentBranch.update(childSlotNum, updated) {
		panic("btree: parent branch key update must succeed on leaf transfer")
	}
}

// selectLeafMergePair はマージで残るノード / 消滅するノードと、それぞれの PageId を返す
//   - 常に左のノードが残る (左兄弟マージなら sibling が残る、右兄弟マージなら child が残る)
func selectLeafMergePair(
	childLeaf, siblingLeaf *leafNode,
	childPageId page.Id,
	sibling siblingInfo,
) (survivor, disappearing *leafNode, survivorPageId, disappearingPageId page.Id) {
	if sibling.isLeft {
		return siblingLeaf, childLeaf, sibling.bufferPage.PageId(), childPageId
	}
	return childLeaf, siblingLeaf, childPageId, sibling.pageId
}

// updateParentForLeafMerge はリーフマージ後の親ブランチノードを更新する
//   - siblingIsLeft: 左の兄弟とのマージなら true (右端のレコード削除ルート)
//   - survivorPageId: マージ後に残るリーフの PageId
func (t *Tree) updateParentForLeafMerge(
	parentBranch *branchNode,
	siblingIsLeft bool,
	survivorPageId page.Id,
	childSlotNum int,
) (underflow bool) {
	if siblingIsLeft {
		parentBranch.delete(parentBranch.numRecords() - 1)
		parentBranch.setRightChildPageId(survivorPageId)
		return !parentBranch.isHalfFull()
	}
	return t.mergeRightSiblingFromParent(parentBranch, survivorPageId, childSlotNum)
}

// relinkLeafAfterMerge は消滅するリーフノードのリンクを残るリーフノードに繋ぎ直す
//   - disappearing: マージにより消滅するリーフノード
//   - survivor: マージ後に残るリーフノード
//   - survivorPageId: survivor の PageId
//   - 呼び出し前提: disappearing.nextPageId のページは touch 済み (書き込み前に PageForWrite 済み)
func (t *Tree) relinkLeafAfterMerge(mtr *buffer.Mtr, disappearing, survivor *leafNode, survivorPageId page.Id) {
	survivor.setNextPageId(disappearing.nextPageId())
	nextPageId := disappearing.nextPageId()
	if nextPageId.IsInvalid() {
		return
	}
	pageNext, err := mtr.PageForWrite(nextPageId)
	if err != nil {
		panic(fmt.Sprintf("btree: PageForWrite for pre-touched leaf next page must not fail (pageId=%v): %v", nextPageId, err))
	}
	nextLeaf := newLeafNode(pageNext)
	nextLeaf.setPrevPageId(survivorPageId)
}

// mergeRightSiblingFromParent は右の兄弟とマージした後の親ブランチノードの更新を行う
//   - parentBranch: 親ブランチノード
//   - survivorPageId: マージ後に残るノードの PageId
//   - childSlotNum: 子ノードのスロット番号
func (t *Tree) mergeRightSiblingFromParent(
	parentBranch *branchNode,
	survivorPageId page.Id,
	childSlotNum int,
) (underflow bool) {
	// 兄弟が RightChild(右端) の場合、親の右端のレコードを削除し、RightChild を子ノードに更新
	if childSlotNum+1 == parentBranch.numRecords() {
		parentBranch.delete(parentBranch.numRecords() - 1)
		parentBranch.setRightChildPageId(survivorPageId)
		return !parentBranch.isHalfFull()
	}

	// 兄弟が RightChild(右端) でない場合、キーを更新してから削除
	childRecord := parentBranch.record(childSlotNum)
	nextRecord := parentBranch.record(childSlotNum + 1)
	updated := NewRecord(childRecord.Header(), nextRecord.Key(), childRecord.NonKey())
	if !parentBranch.update(childSlotNum, updated) {
		panic("btree: parent branch key update must succeed on leaf merge")
	}
	parentBranch.delete(childSlotNum + 1)
	return !parentBranch.isHalfFull()
}
