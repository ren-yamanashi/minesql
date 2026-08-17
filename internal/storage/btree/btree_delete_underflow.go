package btree

import (
	"errors"

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
//   - return:
//   - underflow: 親ブランチノードがアンダーフローしたか
//   - isLeafMerged: リーフノードのマージが発生したか
func (t *Tree) deleteUnderflow(
	mtr *buffer.Mtr,
	branchNode *branchNode,
	childBufPage *buffer.Page,
	childSlotNum int,
) (underflow bool, isLeafMerged bool, err error) {
	// 転送・マージする兄弟ノードを決定
	sibling, err := t.findSibling(mtr, branchNode, childSlotNum)
	if err != nil {
		return false, false, err
	}

	// 子ノードの取得
	childPage, err := mtr.PageForRead(childBufPage.PageId())
	if err != nil {
		return false, false, err
	}

	// リーフノードのアンダーフロー処理
	if nodeType(childPage.Data()) == nodeTypeLeaf {
		uf, lm, err := t.onLeafUnderflow(mtr, branchNode, childBufPage, sibling, childSlotNum)
		return uf, lm, err
	}

	// ブランチノードのアンダーフロー処理
	uf, err := t.onBranchUnderflow(mtr, branchNode, childBufPage, sibling, childSlotNum)
	return uf, isLeafMerged, err
}

// onLeafUnderflow はリーフノードのアンダーフロー処理を行う
//   - parentBranch: 親ブランチノード
//   - childBufPage: アンダーフローが発生した子ノードのバッファページ
//   - sibling: childBufPage の兄弟ノードの情報
//   - childSlotNum: childBufPage が親ブランチノードの子ノードの中で何番目か
//   - return:
//   - underflow: 親ブランチノードがアンダーフローしたか
//   - isLeafMerged: リーフノードのマージが発生したか
func (t *Tree) onLeafUnderflow(
	mtr *buffer.Mtr,
	parentBranch *branchNode,
	childBufPage *buffer.Page,
	sibling siblingInfo,
	childSlotNum int,
) (underflow bool, isLeafMerged bool, err error) {
	pageChild, err := mtr.PageForWrite(childBufPage.PageId())
	if err != nil {
		return false, false, err
	}
	pageSibling, err := mtr.PageForWrite(sibling.pageId)
	if err != nil {
		return false, false, err
	}
	childLeaf := newLeafNode(pageChild)
	siblingLeaf := newLeafNode(pageSibling)

	// 兄弟からレコードを転送できる場合
	if siblingLeaf.canTransferRecord(sibling.isLeft) {
		// 左の兄弟から転送: 左の兄弟の末尾を自分の先頭へ
		if sibling.isLeft {
			lastSlotNum := siblingLeaf.numRecords() - 1
			siblingRecord := siblingLeaf.record(lastSlotNum)
			if !childLeaf.insert(0, siblingRecord) {
				return false, false, errors.New("new leaf node must have space")
			}
			siblingLeaf.delete(lastSlotNum)
			parentRecord := parentBranch.record(childSlotNum - 1)
			updated := NewRecord(parentRecord.Header(), childLeaf.record(0).Key(), parentRecord.NonKey())
			if !parentBranch.update(childSlotNum-1, updated) {
				return false, false, errors.New("failed to update parent branch node key")
			}
			return false, false, nil
		}

		// 右の兄弟から転送: 右の兄弟の先頭を末尾へ
		siblingRecord := siblingLeaf.record(0)
		if !childLeaf.insert(childLeaf.numRecords(), siblingRecord) {
			return false, false, errors.New("new leaf node must have space")
		}
		siblingLeaf.delete(0)
		parentRecord := parentBranch.record(childSlotNum)
		updated := NewRecord(parentRecord.Header(), siblingLeaf.record(0).Key(), parentRecord.NonKey())
		if !parentBranch.update(childSlotNum, updated) {
			return false, false, errors.New("failed to update parent branch node key")
		}
		return false, false, nil
	}

	// 兄弟からレコードを転送できない場合
	// 左の兄弟とマージ: 子(RightChild)のレコードを全て兄弟(左)に移動 (兄弟が残る)
	if sibling.isLeft {
		if !siblingLeaf.transferAllFrom(childLeaf) {
			return false, false, nil // ノードの容量を超えてマージ不可の場合はアンダーフローを許容する
		}
		if err := t.relinkLeafAfterMerge(mtr, childLeaf, siblingLeaf, sibling.bufferPage.PageId()); err != nil {
			return false, false, err
		}
		// 親の右端のレコードは不要になるので削除し、RightChild を兄弟ノードに更新
		parentBranch.delete(parentBranch.numRecords() - 1)
		parentBranch.setRightChildPageId(sibling.bufferPage.PageId())
		if err := fsp.FreePage(mtr, childBufPage.PageId()); err != nil {
			return false, false, err
		}
		return !parentBranch.isHalfFull(), true, nil
	}

	// 右の兄弟とマージ: 兄弟(右)のレコードをすべて子(左)に移動 (子が残る)
	if !childLeaf.transferAllFrom(siblingLeaf) {
		return false, false, nil // ノードの容量を超えてマージ不可の場合はアンダーフローを許容する
	}
	if err := t.relinkLeafAfterMerge(mtr, siblingLeaf, childLeaf, childBufPage.PageId()); err != nil {
		return false, false, err
	}

	uf, err := t.mergeRightSiblingFromParent(parentBranch, childBufPage.PageId(), childSlotNum)
	if err != nil {
		return false, false, err
	}
	if err := fsp.FreePage(mtr, sibling.pageId); err != nil {
		return false, false, err
	}
	return uf, true, nil
}

// onBranchUnderflow はブランチノードのアンダーフロー処理を行う
//   - parentBranch: 親ブランチノード
//   - childBufPage: アンダーフローが発生した子ノードのバッファページ
//   - sibling: childBufPage の兄弟ノードの情報
//   - childSlotNum: childBufPage が親ブランチノードの子ノードの中で何番目か
//   - return: (アンダーフローが発生したかどうか, リーフマージが発生したかどうか)
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

	// 兄弟からレコードを転送できる場合
	if siblingBranch.canTransferRecord(sibling.isLeft) {
		// 左の兄弟から転送: 親の境界キーを子の先頭に下ろし、兄弟の末尾キーを親に上げる
		if sibling.isLeft {
			parentRecord := parentBranch.record(childSlotNum - 1)
			siblingRightChild := siblingBranch.rightChildPageId()
			record := NewRecord([]byte{}, parentRecord.Key(), siblingRightChild.Bytes())
			if !childBranch.insert(0, record) {
				return false, errors.New("new branch node must have space")
			}

			lastSlotNum := siblingBranch.numRecords() - 1
			siblingRecord := siblingBranch.record(lastSlotNum)
			existingRecord := parentBranch.record(childSlotNum - 1)
			updated := NewRecord(existingRecord.Header(), siblingRecord.Key(), existingRecord.NonKey())
			if !parentBranch.update(childSlotNum-1, updated) {
				return false, errors.New("failed to update parent branch node key")
			}
			rightChildPageId, err := page.RestoreId(siblingRecord.NonKey())
			if err != nil {
				return false, err
			}
			siblingBranch.setRightChildPageId(rightChildPageId)
			siblingBranch.delete(lastSlotNum)
			return false, nil
		}

		// 右の兄弟から転送: 親の境界キーを子の末尾に下ろし、兄弟の先頭キーを親に上げる
		parentRecord := parentBranch.record(childSlotNum)
		record := NewRecord([]byte{}, parentRecord.Key(), childBranch.rightChildPageId().Bytes())
		if !childBranch.insert(childBranch.numRecords(), record) {
			return false, errors.New("new branch node must have space")
		}

		siblingRecord := siblingBranch.record(0)
		rightChildPageId, err := page.RestoreId(siblingRecord.NonKey())
		if err != nil {
			return false, err
		}
		childBranch.setRightChildPageId(rightChildPageId)
		existingRecord := parentBranch.record(childSlotNum)
		updated := NewRecord(existingRecord.Header(), siblingRecord.Key(), existingRecord.NonKey())
		if !parentBranch.update(childSlotNum, updated) {
			return false, errors.New("failed to update parent branch node key")
		}
		siblingBranch.delete(0)
		return false, nil
	}

	// 兄弟からレコードを転送できない場合
	// 左の兄弟とマージ: 子(RightChild)のレコードをすべて兄弟(左)に移動 (兄弟が残る)
	if sibling.isLeft {
		parentRecord := parentBranch.record(parentBranch.numRecords() - 1)
		siblingRightChildPageId := siblingBranch.rightChildPageId()
		record := NewRecord([]byte{}, parentRecord.Key(), siblingRightChildPageId.Bytes())
		if !siblingBranch.insert(siblingBranch.numRecords(), record) {
			return false, errors.New("new branch node must have space")
		}
		if !siblingBranch.transferAllFrom(childBranch) {
			// ノードの容量を超えてマージ不可の場合はアンダーフローを許容する
			// 直前に末尾へ挿入した境界キーレコードを取り消し、ノードを元の状態に戻す
			siblingBranch.delete(siblingBranch.numRecords() - 1)
			return false, nil
		}
		siblingBranch.setRightChildPageId(childBranch.rightChildPageId())
		// 親の右端のレコードは不要になるので削除し、RightChild を兄弟ノードに更新
		parentBranch.delete(parentBranch.numRecords() - 1)
		parentBranch.setRightChildPageId(sibling.bufferPage.PageId())
		if err := fsp.FreePage(mtr, childBufPage.PageId()); err != nil {
			return false, err
		}
		return !parentBranch.isHalfFull(), nil
	}

	// 右の兄弟とマージ: 兄弟(右)のレコードをすべて子(左)に移動 (子が残る)
	parentRecord := parentBranch.record(childSlotNum)
	childRightChildPageId := childBranch.rightChildPageId()
	record := NewRecord([]byte{}, parentRecord.Key(), childRightChildPageId.Bytes())
	if !childBranch.insert(childBranch.numRecords(), record) {
		return false, errors.New("new branch node must have space")
	}

	if !childBranch.transferAllFrom(siblingBranch) {
		// ノードの容量を超えてマージ不可の場合はアンダーフローを許容する
		// 直前に末尾へ挿入した境界キーレコードを取り消し、ノードを元の状態に戻す
		childBranch.delete(childBranch.numRecords() - 1)
		return false, nil
	}
	childBranch.setRightChildPageId(siblingBranch.rightChildPageId())

	uf, err := t.mergeRightSiblingFromParent(parentBranch, childBufPage.PageId(), childSlotNum)
	if err != nil {
		return false, err
	}
	if err := fsp.FreePage(mtr, sibling.pageId); err != nil {
		return false, err
	}
	return uf, nil
}

// relinkLeafAfterMerge は消滅するリーフノードのリンクを残るリーフノードに繋ぎ直す
//   - disappearing: マージにより消滅するリーフノード
//   - survivor: マージ後に残るリーフノード
//   - survivorPageId: survivor の PageId
func (t *Tree) relinkLeafAfterMerge(mtr *buffer.Mtr, disappearing, survivor *leafNode, survivorPageId page.Id) error {
	survivor.setNextPageId(disappearing.nextPageId())
	if nextPageId := disappearing.nextPageId(); !nextPageId.IsInvalid() {
		pageNext, err := mtr.PageForWrite(nextPageId)
		if err != nil {
			return err
		}
		nextLeaf := newLeafNode(pageNext)
		nextLeaf.setPrevPageId(survivorPageId)
	}
	return nil
}

// mergeRightSiblingFromParent は右の兄弟とマージした後の親ブランチノードの更新を行う
//   - parentBranch: 親ブランチノード
//   - survivorPageId: マージ後に残るノードの PageId
//   - childSlotNum: 子ノードのスロット番号
func (t *Tree) mergeRightSiblingFromParent(
	parentBranch *branchNode,
	survivorPageId page.Id,
	childSlotNum int,
) (underflow bool, err error) {
	// 兄弟が RightChild(右端) の場合、親の右端のレコードを削除し、RightChild を子ノードに更新
	if childSlotNum+1 == parentBranch.numRecords() {
		parentBranch.delete(parentBranch.numRecords() - 1)
		parentBranch.setRightChildPageId(survivorPageId)
		return !parentBranch.isHalfFull(), nil
	}

	// 兄弟が RightChild(右端) でない場合、キーを更新してから削除
	childRecord := parentBranch.record(childSlotNum)
	nextRecord := parentBranch.record(childSlotNum + 1)
	updated := NewRecord(childRecord.Header(), nextRecord.Key(), childRecord.NonKey())
	if !parentBranch.update(childSlotNum, updated) {
		return false, errors.New("failed to update parent branch node key")
	}
	parentBranch.delete(childSlotNum + 1)
	return !parentBranch.isHalfFull(), nil
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
