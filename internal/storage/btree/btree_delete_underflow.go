package btree

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// siblingInfo は兄弟ノードの情報を表す
type siblingInfo struct {
	pageId     page.PageId
	bufferPage *buffer.BufferPage
	isLeft     bool // true: 兄弟ノードは左の兄弟ノード, false: 兄弟ノードは右の兄弟ノード
}

// deleteUnderflow はアンダーフローした子ノードに対して転送またはマージを行う
//   - branchNode: 親ブランチノード
//   - childBufPage: アンダーフローが発生した子ノードのバッファページ
//   - childSlotNum: 子ノードのスロット番号
//   - return:
//   - underflow: 親ブランチノードがアンダーフローしたか
//   - isLeafMerged: リーフノードのマージが発生したか
func (bt *Btree) deleteUnderflow(
	branchNode *node.BranchNode,
	childBufPage *buffer.BufferPage,
	childSlotNum int,
) (underflow bool, isLeafMerged bool, err error) {
	// 転送・マージする兄弟ノードを決定
	sibling, err := bt.findSibling(branchNode, childSlotNum)
	if err != nil {
		return false, false, err
	}

	// 兄弟ノードの取得
	siblingBufPage, err := bt.bufferPool.FetchPage(sibling.pageId)
	if err != nil {
		return false, false, err
	}
	sibling.bufferPage = siblingBufPage
	defer bt.bufferPool.UnRefPage(sibling.pageId)

	// 子ノードの取得
	childPage, err := bt.bufferPool.GetReadPage(childBufPage.PageId)
	if err != nil {
		return false, false, err
	}

	// リーフノードのアンダーフロー処理
	if bytes.Equal(node.GetNodeType(childPage), node.NodeTypeLeaf) {
		uf, lm, err := bt.onLeafUnderflow(branchNode, childBufPage, sibling, childSlotNum)
		return uf, lm, err
	}

	// ブランチノードのアンダーフロー処理
	uf, err := bt.onBranchUnderflow(branchNode, childBufPage, sibling, childSlotNum)
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
func (bt *Btree) onLeafUnderflow(
	parentBranch *node.BranchNode,
	childBufPage *buffer.BufferPage,
	sibling siblingInfo,
	childSlotNum int,
) (underflow bool, isLeafMerged bool, err error) {
	pageChild, err := bt.bufferPool.GetWritePage(childBufPage.PageId)
	if err != nil {
		return false, false, err
	}
	pageSibling, err := bt.bufferPool.GetWritePage(sibling.pageId)
	if err != nil {
		return false, false, err
	}
	childLeaf := node.NewLeafNode(pageChild)
	siblingLeaf := node.NewLeafNode(pageSibling)

	// 兄弟からレコードを転送できる場合
	if siblingLeaf.CanTransferRecord(sibling.isLeft) {
		// 左の兄弟から転送: 左の兄弟の末尾を自分の先頭へ
		if sibling.isLeft {
			lastSlotNum := siblingLeaf.NumRecords() - 1
			siblingRecord := siblingLeaf.Record(lastSlotNum)
			if !childLeaf.Insert(0, siblingRecord) {
				return false, false, errors.New("new leaf node must have space")
			}
			siblingLeaf.Delete(lastSlotNum)
			parentRecord := parentBranch.Record(childSlotNum - 1)
			updated := node.NewRecord(parentRecord.Header(), childLeaf.Record(0).Key(), parentRecord.NonKey())
			if !parentBranch.Update(childSlotNum-1, updated) {
				return false, false, errors.New("failed to update parent branch node key")
			}
			return false, false, nil
		}

		// 右の兄弟から転送: 右の兄弟の先頭を末尾へ
		siblingRecord := siblingLeaf.Record(0)
		if !childLeaf.Insert(childLeaf.NumRecords(), siblingRecord) {
			return false, false, errors.New("new leaf node must have space")
		}
		siblingLeaf.Delete(0)
		parentRecord := parentBranch.Record(childSlotNum)
		updated := node.NewRecord(parentRecord.Header(), siblingLeaf.Record(0).Key(), parentRecord.NonKey())
		if !parentBranch.Update(childSlotNum, updated) {
			return false, false, errors.New("failed to update parent branch node key")
		}
		return false, false, nil
	}

	// 兄弟からレコードを転送できない場合
	// 左の兄弟とマージ: 子(RightChild)のレコードを全て兄弟(左)に移動 (兄弟が残る)
	if sibling.isLeft {
		if !siblingLeaf.TransferAllFrom(childLeaf) {
			return false, false, nil // ノードの容量を超えてマージ不可の場合はアンダーフローを許容する
		}
		if err := bt.relinkLeafAfterMerge(childLeaf, siblingLeaf, sibling.bufferPage.PageId); err != nil {
			return false, false, err
		}
		// 親の右端のレコードは不要になるので削除し、RightChild を兄弟ノードに更新
		parentBranch.Delete(parentBranch.NumRecords() - 1)
		parentBranch.SetRightChildPageId(sibling.bufferPage.PageId)
		return !parentBranch.IsHalfFull(), true, nil
	}

	// 右の兄弟とマージ: 兄弟(右)のレコードをすべて子(左)に移動 (子が残る)
	if !childLeaf.TransferAllFrom(siblingLeaf) {
		return false, false, nil // ノードの容量を超えてマージ不可の場合はアンダーフローを許容する
	}
	if err := bt.relinkLeafAfterMerge(siblingLeaf, childLeaf, childBufPage.PageId); err != nil {
		return false, false, err
	}

	uf, err := bt.mergeRightSiblingFromParent(parentBranch, childBufPage.PageId, childSlotNum)
	if err != nil {
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
func (bt *Btree) onBranchUnderflow(
	parentBranch *node.BranchNode,
	childBufPage *buffer.BufferPage,
	sibling siblingInfo,
	childSlotNum int,
) (underflow bool, err error) {
	pageChild, err := bt.bufferPool.GetWritePage(childBufPage.PageId)
	if err != nil {
		return false, err
	}
	pageSibling, err := bt.bufferPool.GetWritePage(sibling.pageId)
	if err != nil {
		return false, err
	}
	childBranch := node.NewBranchNode(pageChild)
	siblingBranch := node.NewBranchNode(pageSibling)

	// 兄弟からレコードを転送できる場合
	if siblingBranch.CanTransferRecord(sibling.isLeft) {
		// 左の兄弟から転送: 親の境界キーを子の先頭に下ろし、兄弟の末尾キーを親に上げる
		if sibling.isLeft {
			parentRecord := parentBranch.Record(childSlotNum - 1)
			siblingRightChild := siblingBranch.RightChildPageId()
			record := node.NewRecord([]byte{}, parentRecord.Key(), siblingRightChild.ToBytes())
			if !childBranch.Insert(0, record) {
				return false, errors.New("new branch node must have space")
			}

			lastSlotNum := siblingBranch.NumRecords() - 1
			siblingRecord := siblingBranch.Record(lastSlotNum)
			existingRecord := parentBranch.Record(childSlotNum - 1)
			updated := node.NewRecord(existingRecord.Header(), siblingRecord.Key(), existingRecord.NonKey())
			if !parentBranch.Update(childSlotNum-1, updated) {
				return false, errors.New("failed to update parent branch node key")
			}
			rightChildPageId, err := page.RestorePageId(siblingRecord.NonKey())
			if err != nil {
				return false, err
			}
			siblingBranch.SetRightChildPageId(rightChildPageId)
			siblingBranch.Delete(lastSlotNum)
			return false, nil
		}

		// 右の兄弟から転送: 親の境界キーを子の末尾に下ろし、兄弟の先頭キーを親に上げる
		parentRecord := parentBranch.Record(childSlotNum)
		record := node.NewRecord([]byte{}, parentRecord.Key(), childBranch.RightChildPageId().ToBytes())
		if !childBranch.Insert(childBranch.NumRecords(), record) {
			return false, errors.New("new branch node must have space")
		}

		siblingRecord := siblingBranch.Record(0)
		rightChildPageId, err := page.RestorePageId(siblingRecord.NonKey())
		if err != nil {
			return false, err
		}
		childBranch.SetRightChildPageId(rightChildPageId)
		existingRecord := parentBranch.Record(childSlotNum)
		updated := node.NewRecord(existingRecord.Header(), siblingRecord.Key(), existingRecord.NonKey())
		if !parentBranch.Update(childSlotNum, updated) {
			return false, errors.New("failed to update parent branch node key")
		}
		siblingBranch.Delete(0)
		return false, nil
	}

	// 兄弟からレコードを転送できない場合
	// 左の兄弟とマージ: 子(RightChild)のレコードをすべて兄弟(左)に移動 (兄弟が残る)
	if sibling.isLeft {
		parentRecord := parentBranch.Record(parentBranch.NumRecords() - 1)
		siblingRightChildPageId := siblingBranch.RightChildPageId()
		record := node.NewRecord([]byte{}, parentRecord.Key(), siblingRightChildPageId.ToBytes())
		if !siblingBranch.Insert(siblingBranch.NumRecords(), record) {
			return false, errors.New("new branch node must have space")
		}
		siblingBranch.TransferAllFrom(childBranch)
		siblingBranch.SetRightChildPageId(childBranch.RightChildPageId())
		// 親の右端のレコードは不要になるので削除し、RightChild を兄弟ノードに更新
		parentBranch.Delete(parentBranch.NumRecords() - 1)
		parentBranch.SetRightChildPageId(sibling.bufferPage.PageId)
		return !parentBranch.IsHalfFull(), nil
	}

	// 右の兄弟とマージ: 兄弟(右)のレコードをすべて子(左)に移動 (子が残る)
	parentRecord := parentBranch.Record(childSlotNum)
	childRightChildPageId := childBranch.RightChildPageId()
	record := node.NewRecord([]byte{}, parentRecord.Key(), childRightChildPageId.ToBytes())
	if !childBranch.Insert(childBranch.NumRecords(), record) {
		return false, errors.New("new branch node must have space")
	}

	childBranch.TransferAllFrom(siblingBranch)
	childBranch.SetRightChildPageId(siblingBranch.RightChildPageId())

	return bt.mergeRightSiblingFromParent(parentBranch, childBufPage.PageId, childSlotNum)
}

// relinkLeafAfterMerge は消滅するリーフノードのリンクを残るリーフノードに繋ぎ直す
//   - disappearing: マージにより消滅するリーフノード
//   - survivor: マージ後に残るリーフノード
//   - survivorPageId: survivor の PageId
func (bt *Btree) relinkLeafAfterMerge(disappearing, survivor *node.LeafNode, survivorPageId page.PageId) error {
	survivor.SetNextPageId(disappearing.NextPageId())
	if nextPageId := disappearing.NextPageId(); !nextPageId.IsInvalid() {
		defer bt.bufferPool.UnRefPage(nextPageId)
		pageNext, err := bt.bufferPool.GetWritePage(nextPageId)
		if err != nil {
			return err
		}
		nextLeaf := node.NewLeafNode(pageNext)
		nextLeaf.SetPrevPageId(survivorPageId)
	}
	return nil
}

// mergeRightSiblingFromParent は右の兄弟とマージした後の親ブランチノードの更新を行う
//   - parentBranch: 親ブランチノード
//   - survivorPageId: マージ後に残るノードの PageId
//   - childSlotNum: 子ノードのスロット番号
func (bt *Btree) mergeRightSiblingFromParent(
	parentBranch *node.BranchNode,
	survivorPageId page.PageId,
	childSlotNum int,
) (underflow bool, err error) {
	// 兄弟が RightChild(右端) の場合、親の右端のレコードを削除し、RightChild を子ノードに更新
	if childSlotNum+1 == parentBranch.NumRecords() {
		parentBranch.Delete(parentBranch.NumRecords() - 1)
		parentBranch.SetRightChildPageId(survivorPageId)
		return !parentBranch.IsHalfFull(), nil
	}

	// 兄弟が RightChild(右端) でない場合、キーを更新してから削除
	childRecord := parentBranch.Record(childSlotNum)
	nextRecord := parentBranch.Record(childSlotNum + 1)
	updated := node.NewRecord(childRecord.Header(), nextRecord.Key(), childRecord.NonKey())
	if !parentBranch.Update(childSlotNum, updated) {
		return false, errors.New("failed to update parent branch node key")
	}
	parentBranch.Delete(childSlotNum + 1)
	return !parentBranch.IsHalfFull(), nil
}

// findSibling は転送・マージ対象の兄弟ノードを決定する
func (bt *Btree) findSibling(branchNode *node.BranchNode, childSlotNum int) (siblingInfo, error) {
	if childSlotNum < branchNode.NumRecords() {
		siblingPageId, err := branchNode.ChildPageId(childSlotNum + 1)
		return siblingInfo{pageId: siblingPageId, isLeft: false}, err
	}
	siblingPageId, err := branchNode.ChildPageId(childSlotNum - 1)
	return siblingInfo{pageId: siblingPageId, isLeft: true}, err
}
