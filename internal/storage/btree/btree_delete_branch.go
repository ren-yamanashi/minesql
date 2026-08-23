package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// onBranchUnderflow はブランチノードのアンダーフロー処理を行う (3 フェーズ)
//   - 判定 → touch → 書き込みの順に実行し、書き込みフェーズは失敗しない
func (t *Tree) onBranchUnderflow(
	mtr *buffer.Mtr,
	parentBranch *branchNode,
	childBufPage *buffer.Page,
	sibling siblingInfo,
	childSlotNum int,
) (underflow bool, err error) {
	pageChild, err := mtr.PageForWrite(childBufPage.PageId())
	if err != nil {
		return false, err
	}
	pageSibling, err := mtr.PageForWrite(sibling.pageId)
	if err != nil {
		return false, err
	}
	childBranch := newBranchNode(pageChild)
	siblingBranch := newBranchNode(pageSibling)

	// 判定フェーズ (読みのみ): 転送 / マージ / アンダーフロー許容
	if siblingBranch.canTransferRecord(sibling.isLeft) && canUpdateParentForBranchTransfer(parentBranch, siblingBranch, sibling.isLeft, childSlotNum) {
		transferBranch(childBranch, siblingBranch, parentBranch, sibling.isLeft, childSlotNum)
		return false, nil
	}
	survivor, disappearing, survivorPageId, disappearingPageId := selectBranchMergePair(childBranch, siblingBranch, childBufPage.PageId(), sibling)
	boundaryKey := boundaryKeyForBranchMerge(parentBranch, sibling.isLeft, childSlotNum)
	if !canMergeBranchPair(survivor, disappearing, boundaryKey) {
		return false, nil // ノードの容量を超えてマージ不可の場合はアンダーフローを許容する
	}
	if !canUpdateParentForRightMerge(parentBranch, sibling.isLeft, childSlotNum) {
		return false, nil // 親ブランチの境界キー差し替えが収まらない場合はアンダーフローを許容する
	}

	// touch フェーズ (書き込み開始前): 解放対象を取得する
	plan, err := fsp.TouchFreeSegmentPage(mtr, t.branchSegmentHeaderAt(), disappearingPageId)
	if err != nil {
		return false, err
	}

	// 書き込みフェーズ: 失敗しない
	mergeBranchPair(survivor, disappearing, boundaryKey)
	uf := t.updateParentForBranchMerge(parentBranch, sibling.isLeft, survivorPageId, childSlotNum)
	fsp.WriteFreeSegmentPage(mtr, plan)
	return uf, nil
}

// transferBranch はブランチノード間の 1 レコード転送 (親経由のローテーション) を行う
func transferBranch(
	childBranch, siblingBranch *branchNode,
	parentBranch *branchNode,
	siblingIsLeft bool,
	childSlotNum int,
) {
	if siblingIsLeft {
		// 左の兄弟から転送: 親の境界キーを子の先頭に下ろし、兄弟の末尾キーを親に上げる
		parentRecord := parentBranch.record(childSlotNum - 1)
		siblingRightChild := siblingBranch.rightChildPageId()
		record := NewRecord([]byte{}, parentRecord.Key(), siblingRightChild.Bytes())
		if !childBranch.insert(0, record) {
			panic("btree: child branch must have space for transfer from left sibling")
		}
		lastSlotNum := siblingBranch.numRecords() - 1
		siblingRecord := siblingBranch.record(lastSlotNum)
		existingRecord := parentBranch.record(childSlotNum - 1)
		updated := NewRecord(existingRecord.Header(), siblingRecord.Key(), existingRecord.NonKey())
		if !parentBranch.update(childSlotNum-1, updated) {
			panic("btree: parent branch key update must succeed on branch transfer from left")
		}
		rightChildPageId, err := page.RestoreId(siblingRecord.NonKey())
		if err != nil {
			panic("btree: corrupted branch record NonKey for right child page id (left transfer)")
		}
		siblingBranch.setRightChildPageId(rightChildPageId)
		siblingBranch.delete(lastSlotNum)
		return
	}
	// 右の兄弟から転送: 親の境界キーを子の末尾に下ろし、兄弟の先頭キーを親に上げる
	parentRecord := parentBranch.record(childSlotNum)
	record := NewRecord([]byte{}, parentRecord.Key(), childBranch.rightChildPageId().Bytes())
	if !childBranch.insert(childBranch.numRecords(), record) {
		panic("btree: child branch must have space for transfer from right sibling")
	}
	siblingRecord := siblingBranch.record(0)
	rightChildPageId, err := page.RestoreId(siblingRecord.NonKey())
	if err != nil {
		panic("btree: corrupted branch record NonKey for right child page id (right transfer)")
	}
	childBranch.setRightChildPageId(rightChildPageId)
	existingRecord := parentBranch.record(childSlotNum)
	updated := NewRecord(existingRecord.Header(), siblingRecord.Key(), existingRecord.NonKey())
	if !parentBranch.update(childSlotNum, updated) {
		panic("btree: parent branch key update must succeed on branch transfer from right")
	}
	siblingBranch.delete(0)
}

// canUpdateParentForLeafTransfer はリーフ転送に伴う親ブランチの境界キー差し替えが親の空き容量に収まるかを、書き込まずに返す
func canUpdateParentForLeafTransfer(parentBranch *branchNode, siblingLeaf *leafNode, siblingIsLeft bool, childSlotNum int) bool {
	if siblingIsLeft {
		// 左の兄弟の末尾を子の先頭へ転送: 新しい境界キーは兄弟の末尾レコードのキー
		newKey := siblingLeaf.record(siblingLeaf.numRecords() - 1).Key()
		return canUpdateParentBoundaryKey(parentBranch, childSlotNum-1, newKey)
	}
	// 右の兄弟の先頭を子の末尾へ転送: 新しい境界キーは兄弟の 2 番目のレコードのキー (=先頭削除後の先頭)
	return canUpdateParentBoundaryKey(parentBranch, childSlotNum, siblingLeaf.record(1).Key())
}

// canUpdateParentForBranchTransfer はブランチ転送に伴う親ブランチの境界キー差し替えが親の空き容量に収まるかを、書き込まずに返す
func canUpdateParentForBranchTransfer(parentBranch, siblingBranch *branchNode, siblingIsLeft bool, childSlotNum int) bool {
	if siblingIsLeft {
		// 左の兄弟から転送: 新しい親のキーは兄弟の末尾レコードのキー
		newKey := siblingBranch.record(siblingBranch.numRecords() - 1).Key()
		return canUpdateParentBoundaryKey(parentBranch, childSlotNum-1, newKey)
	}
	// 右の兄弟から転送: 新しい親のキーは兄弟の先頭レコードのキー
	return canUpdateParentBoundaryKey(parentBranch, childSlotNum, siblingBranch.record(0).Key())
}

// canUpdateParentForRightMerge は右の兄弟とのマージ後の親ブランチ更新が親の空き容量に収まるかを、書き込まずに返す
//   - 左マージ / 右マージ (兄弟が RightChild) は親レコードを削除するのみで update を行わないため常に true
func canUpdateParentForRightMerge(parentBranch *branchNode, siblingIsLeft bool, childSlotNum int) bool {
	if siblingIsLeft {
		return true
	}
	if childSlotNum+1 == parentBranch.numRecords() {
		return true
	}
	return canUpdateParentBoundaryKey(parentBranch, childSlotNum, parentBranch.record(childSlotNum+1).Key())
}

// canUpdateParentBoundaryKey は親ブランチの指定スロットのレコードのキーを newKey に差し替えたときに親の空き容量に収まるかを、書き込まずに返す
func canUpdateParentBoundaryKey(parentBranch *branchNode, slotNum int, newKey []byte) bool {
	existing := parentBranch.record(slotNum)
	updated := NewRecord(existing.Header(), newKey, existing.NonKey())
	return parentBranch.canUpdate(slotNum, updated)
}

// selectBranchMergePair はマージで残るブランチノード / 消滅するブランチノードと、それぞれの PageId を返す
//   - 常に左のノードが残る
func selectBranchMergePair(
	childBranch, siblingBranch *branchNode,
	childPageId page.Id,
	sibling siblingInfo,
) (survivor, disappearing *branchNode, survivorPageId, disappearingPageId page.Id) {
	if sibling.isLeft {
		return siblingBranch, childBranch, sibling.bufferPage.PageId(), childPageId
	}
	return childBranch, siblingBranch, childPageId, sibling.pageId
}

// boundaryKeyForBranchMerge はマージ時に survivor の末尾へ挿入する境界キー (親の対応するレコードのキー) を返す
func boundaryKeyForBranchMerge(parentBranch *branchNode, siblingIsLeft bool, childSlotNum int) []byte {
	if siblingIsLeft {
		return parentBranch.record(parentBranch.numRecords() - 1).Key()
	}
	return parentBranch.record(childSlotNum).Key()
}

// canMergeBranchPair は survivor + 境界キーレコード + disappearing の全レコードが survivor に収まるかを、書き込まずに返す
func canMergeBranchPair(survivor, disappearing *branchNode, boundaryKey []byte) bool {
	boundaryRecord := NewRecord([]byte{}, boundaryKey, disappearing.rightChildPageId().Bytes())
	requiredForBoundary := len(boundaryRecord.Bytes()) + slottedPagePointerSize
	if survivor.body.freeSpace() < requiredForBoundary {
		return false
	}
	// boundary を挿入した後の空き容量で disappearing の全レコードを収められるか
	freeAfterBoundary := survivor.body.freeSpace() - requiredForBoundary
	srcNumSlots := disappearing.numRecords()
	if srcNumSlots == 0 {
		return true
	}
	var totalDataSize int
	for i := range srcNumSlots {
		totalDataSize += len(disappearing.body.cell(i))
	}
	requiredForRest := srcNumSlots*slottedPagePointerSize + totalDataSize
	return freeAfterBoundary >= requiredForRest
}

// mergeBranchPair は survivor に境界キーレコード + disappearing の全レコードを取り込み、RightChild を更新する
//   - 呼び出し前提: canMergeBranchPair が true であること
func mergeBranchPair(survivor, disappearing *branchNode, boundaryKey []byte) {
	boundaryRecord := NewRecord([]byte{}, boundaryKey, survivor.rightChildPageId().Bytes())
	if !survivor.insert(survivor.numRecords(), boundaryRecord) {
		panic("btree: survivor branch must have space for boundary record after canMergeBranchPair check")
	}
	if !survivor.transferAllFrom(disappearing) {
		panic("btree: survivor branch must have space for merged records after canMergeBranchPair check")
	}
	survivor.setRightChildPageId(disappearing.rightChildPageId())
}

// updateParentForBranchMerge はブランチマージ後の親ブランチノードを更新する
func (t *Tree) updateParentForBranchMerge(
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
