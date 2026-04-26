package btree

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// Delete は B+Tree からレコードを削除する
func (bt *BTree) Delete(bp *buffer.BufferPool, key []byte) error {
	// メタページを取得
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	metaData, err := bp.GetReadPageData(bt.MetaPageId)
	if err != nil {
		return err
	}
	meta := newMetaPage(page.NewPage(metaData))

	// ルートページを取得
	rootPageId := meta.rootPageId()
	rootPageBuf, err := bp.FetchPage(rootPageId)
	if err != nil {
		return err
	}

	_, leafMerged, err := bt.deleteRecursively(bp, rootPageBuf, key)
	if err != nil {
		return err
	}

	// ルートノードがブランチノードで、子が 1 つになった場合 (=ブランチノード1, リーフノード1 になった場合)、子をルートにする
	rootCollapsed := false
	rootData, err := bp.GetReadPageData(rootPageBuf.PageId)
	if err != nil {
		return err
	}
	nodeType := node.GetNodeType(page.NewPage(rootData).Body)
	if bytes.Equal(nodeType, node.NodeTypeBranch) {
		branch := node.NewBranch(page.NewPage(rootData).Body)
		if branch.NumRecords() == 0 {
			rootCollapsed = true
		}
	}

	// リーフマージもルート縮退も発生しなかった場合、終了
	if !leafMerged && !rootCollapsed {
		return nil
	}

	// リーフマージまたはルート縮退が発生した場合 (この二つは同時に発生する可能性もある)
	metaWriteData, err := bp.GetWritePageData(bt.MetaPageId)
	if err != nil {
		return err
	}
	meta = newMetaPage(page.NewPage(metaWriteData))

	// リーフマージが発生した場合、メタページのリーフページ数を更新
	if leafMerged {
		meta.setLeafPageCount(meta.leafPageCount() - 1)
	}

	// ルートノードの縮退が発生した場合、子ノードをルートにしてメタページを更新
	if rootCollapsed {
		// 子が 1 つになった場合、右端の子をルートにする
		branch := node.NewBranch(page.NewPage(rootData).Body)
		newRootPageId := branch.ChildPageIdAt(0)
		meta.setRootPageId(newRootPageId)
		meta.setHeight(meta.height() - 1)
	}

	return nil
}

// deleteRecursively は再帰的にノードを辿ってレコードを削除する
//
// 戻り値: (アンダーフローが発生したかどうか, リーフマージが発生したかどうか, エラー)
func (bt *BTree) deleteRecursively(
	bp *buffer.BufferPool,
	nodeBuffer *buffer.BufferPage,
	key []byte,
) (underflow bool, leafMerged bool, err error) {
	nodeData, err := bp.GetReadPageData(nodeBuffer.PageId)
	if err != nil {
		return false, false, err
	}
	nodeType := node.GetNodeType(page.NewPage(nodeData).Body)

	switch {
	case bytes.Equal(nodeType, node.NodeTypeLeaf):
		underflow, err = bt.deleteFromLeaf(bp, nodeBuffer, key)
		return underflow, false, err
	case bytes.Equal(nodeType, node.NodeTypeBranch):
		return bt.deleteFromBranch(bp, nodeBuffer, key)
	default:
		panic("unknown node type")
	}
}

// siblingInfo は兄弟ノードの情報を表す
type siblingInfo struct {
	pageId     page.PageId
	bufferPage *buffer.BufferPage
	isLeft     bool // true: 兄弟ノードは左の兄弟ノード, false: 兄弟ノードは右の兄弟ノード
}

// deleteFromBranch はブランチノードから再帰的にレコードを削除する
//
// bp: バッファプール
//
// nodeBuffer: 削除処理を行うノードのバッファページ (ブランチノードのバッファページ)
//
// key: 削除するキー
//
// 戻り値: (アンダーフローが発生したかどうか, リーフマージが発生したかどうか, エラー)
func (bt *BTree) deleteFromBranch(
	bp *buffer.BufferPool,
	nodeBuffer *buffer.BufferPage,
	key []byte,
) (underflow bool, leafMerged bool, err error) {
	nodeWriteData, err := bp.GetWritePageData(nodeBuffer.PageId)
	if err != nil {
		return false, false, err
	}
	branch := node.NewBranch(page.NewPage(nodeWriteData).Body)
	childSlotNum := branch.SearchChildSlotNum(key)
	childPageId := branch.ChildPageIdAt(childSlotNum)
	childNodeBuffer, err := bp.FetchPage(childPageId)
	if err != nil {
		return false, false, err
	}
	defer bp.UnRefPage(childPageId)

	// 子ノードに対して削除処理を再帰的に実行
	underflow, leafMerged, err = bt.deleteRecursively(bp, childNodeBuffer, key)
	if err != nil {
		return false, false, err
	}

	// 子ノードでアンダーフローが発生しなかった場合
	if !underflow {
		return false, leafMerged, nil
	}

	// 子ノードでアンダーフローが発生した場合

	// 転送・マージする兄弟ノードを決定
	sibling := func() siblingInfo {
		if childSlotNum < branch.NumRecords() {
			siblingPageId := branch.ChildPageIdAt(childSlotNum + 1)
			return siblingInfo{pageId: siblingPageId, isLeft: false}
		}
		siblingPageId := branch.ChildPageIdAt(childSlotNum - 1)
		return siblingInfo{pageId: siblingPageId, isLeft: true}
	}()

	siblingBuffer, err := bp.FetchPage(sibling.pageId)
	if err != nil {
		return false, false, err
	}
	sibling.bufferPage = siblingBuffer
	defer bp.UnRefPage(sibling.pageId)

	childReadData, err := bp.GetReadPageData(childNodeBuffer.PageId)
	if err != nil {
		return false, false, err
	}
	if bytes.Equal(node.GetNodeType(page.NewPage(childReadData).Body), node.NodeTypeLeaf) {
		uf, lm, err := bt.resolveLeafUnderflow(bp, branch, childNodeBuffer, sibling, childSlotNum)
		return uf, leafMerged || lm, err
	}
	uf, err := bt.resolveBranchUnderflow(bp, branch, childNodeBuffer, sibling, childSlotNum)
	return uf, leafMerged, err
}

// deleteFromLeaf はリーフノードからレコードを削除する
//
// 戻り値: (アンダーフローが発生したかどうか, エラー)
func (bt *BTree) deleteFromLeaf(bp *buffer.BufferPool, nodeBuffer *buffer.BufferPage, key []byte) (underflow bool, err error) {
	// 削除すべきレコード (レコードが格納されているスロット番号) を特定
	nodeWriteData, err := bp.GetWritePageData(nodeBuffer.PageId)
	if err != nil {
		return false, err
	}
	leaf := node.NewLeaf(page.NewPage(nodeWriteData).Body)
	slotNum, found := leaf.SearchSlotNum(key)
	if !found {
		return false, ErrKeyNotFound
	}

	// レコードを削除
	leaf.Delete(slotNum)

	// アンダーフローが発生したかどうかを判定
	return !leaf.IsHalfFull(), nil
}

// resolveLeafUnderflow はリーフノードのアンダーフロー処理を行う
//
// parentBranch: 親のブランチノード
//
// childBuffer: アンダーフローが発生した子ノードのバッファページ (リーフノードのバッファページ)
//
// sibling: childBuffer と兄弟ノードの情報
//
// childSlotNum: childBuffer が親のブランチノードの子ノードの中で何番目か
//
// 戻り値: (アンダーフローが発生したかどうか, リーフマージが発生したかどうか, エラー)
func (bt *BTree) resolveLeafUnderflow(
	bp *buffer.BufferPool,
	parentBranch *node.Branch,
	childBuffer *buffer.BufferPage,
	sibling siblingInfo,
	childSlotNum int,
) (underflow bool, leafMerged bool, err error) {
	childWriteData, err := bp.GetWritePageData(childBuffer.PageId)
	if err != nil {
		return false, false, err
	}
	siblingWriteData, err := bp.GetWritePageData(sibling.bufferPage.PageId)
	if err != nil {
		return false, false, err
	}
	childNode := node.NewLeaf(page.NewPage(childWriteData).Body)
	siblingNode := node.NewLeaf(page.NewPage(siblingWriteData).Body)

	// 兄弟からレコードを転送できる場合
	if siblingNode.CanTransferRecord(sibling.isLeft) {
		if sibling.isLeft {
			// 左の兄弟から転送: 左の兄弟の末尾レコードを先頭に移動
			lastIndex := siblingNode.NumRecords() - 1
			record := siblingNode.RecordAt(lastIndex)
			childNode.Insert(0, record)
			siblingNode.Delete(lastIndex)
			if !parentBranch.Update(childSlotNum-1, childNode.RecordAt(0).KeyBytes()) {
				return false, false, errors.New("failed to update parent branch node key")
			}
		} else {
			// 右の兄弟から転送: 右の兄弟の先頭レコードを末尾に移動
			record := siblingNode.RecordAt(0)
			childNode.Insert(childNode.NumRecords(), record)
			siblingNode.Delete(0)
			if !parentBranch.Update(childSlotNum, siblingNode.RecordAt(0).KeyBytes()) {
				return false, false, errors.New("failed to update parent branch node key")
			}
		}
		return false, false, nil
	}

	// 兄弟からレコードを転送できない場合はマージする
	// ただし、空き容量不足でマージできない場合はアンダーフローを許容してそのまま返す
	if sibling.isLeft {
		// 左の兄弟とマージ: 子(RightChild)のレコードをすべて兄弟(左)に移動、兄弟が残る
		if !siblingNode.TransferAllFrom(childNode) {
			// ノードの容量を超えてマージ不可の場合はアンダーフローを許容する
			return false, false, nil
		}
		siblingNode.SetNextPageId(childNode.NextPageId())
		// 消滅する childNode の次のリーフの prevPageId を sibling に更新する
		if nextPageId := childNode.NextPageId(); nextPageId != nil {
			defer bp.UnRefPage(*nextPageId)
			nextWriteData, err := bp.GetWritePageData(*nextPageId)
			if err != nil {
				return false, false, err
			}
			nextLeaf := node.NewLeaf(page.NewPage(nextWriteData).Body)
			nextLeaf.SetPrevPageId(&sibling.bufferPage.PageId)
		}
		// 親の右端のレコードは不要になるので削除し、RightChild を兄弟ノードに更新
		parentBranch.Delete(parentBranch.NumRecords() - 1)
		parentBranch.SetRightChildPageId(sibling.bufferPage.PageId)
	} else {
		// 右の兄弟とマージ: 兄弟(右)のレコードをすべて子(左)に移動、子が残る
		if !childNode.TransferAllFrom(siblingNode) {
			// ノードの容量を超えてマージ不可の場合はアンダーフローを許容する
			return false, false, nil
		}
		childNode.SetNextPageId(siblingNode.NextPageId())
		// 消滅する siblingNode の次のリーフの prevPageId を child に更新する
		if nextPageId := siblingNode.NextPageId(); nextPageId != nil {
			defer bp.UnRefPage(*nextPageId)
			nextWriteData, err := bp.GetWritePageData(*nextPageId)
			if err != nil {
				return false, false, err
			}
			nextLeaf := node.NewLeaf(page.NewPage(nextWriteData).Body)
			nextLeaf.SetPrevPageId(&childBuffer.PageId)
		}

		if childSlotNum+1 == parentBranch.NumRecords() {
			// 兄弟が RightChild(右端) の場合、親の右端のレコードを削除し、RightChild を子ノードに更新
			parentBranch.Delete(parentBranch.NumRecords() - 1)
			parentBranch.SetRightChildPageId(childBuffer.PageId)
		} else {
			// 兄弟が RightChild(右端) でない場合、キーを更新してから削除
			nextKey := parentBranch.RecordAt(childSlotNum + 1).KeyBytes()
			if !parentBranch.Update(childSlotNum, nextKey) {
				return false, false, errors.New("failed to update parent branch node key")
			}
			parentBranch.Delete(childSlotNum + 1) // 右の兄弟を削除するので `childIndex + 1`
		}
	}

	return !parentBranch.IsHalfFull(), true, nil
}

// resolveBranchUnderflow はブランチノードのアンダーフロー処理を行う
//
// parentBranch: 親のブランチノード
//
// childBuffer: アンダーフローが発生した子ノードのバッファページ (ブランチノードのバッファページ)
//
// sibling: childBuffer の兄弟ノードの情報
//
// childSlotNum: childBuffer が親のブランチノードの子ノードの中で何番目か
func (bt *BTree) resolveBranchUnderflow(
	bp *buffer.BufferPool,
	parentBranch *node.Branch,
	childBuffer *buffer.BufferPage,
	sibling siblingInfo,
	childSlotNum int,
) (underflow bool, err error) {
	childWriteData, err := bp.GetWritePageData(childBuffer.PageId)
	if err != nil {
		return false, err
	}
	siblingWriteData, err := bp.GetWritePageData(sibling.bufferPage.PageId)
	if err != nil {
		return false, err
	}
	childNode := node.NewBranch(page.NewPage(childWriteData).Body)
	siblingNode := node.NewBranch(page.NewPage(siblingWriteData).Body)

	// 兄弟からレコードを転送できる場合
	if siblingNode.CanTransferRecord(sibling.isLeft) {
		if sibling.isLeft {
			// 左の兄弟から転送: 親の境界キーを子の先頭に下ろし、兄弟の末尾キーを親に上げる
			parentRecord := parentBranch.RecordAt(childSlotNum - 1)
			siblingRightChild := siblingNode.RightChildPageId()
			childNode.Insert(0, node.NewRecord(nil, parentRecord.KeyBytes(), siblingRightChild.ToBytes()))

			lastIndex := siblingNode.NumRecords() - 1
			siblingLastRecord := siblingNode.RecordAt(lastIndex)
			if !parentBranch.Update(childSlotNum-1, siblingLastRecord.KeyBytes()) {
				return false, errors.New("failed to update parent branch node key")
			}
			siblingNode.SetRightChildPageId(page.RestorePageIdFromBytes(siblingLastRecord.NonKeyBytes()))
			siblingNode.Delete(lastIndex)
		} else {
			// 右の兄弟から転送: 親の境界キーを子の末尾に下ろし、兄弟の先頭キーを親に上げる
			parentRecord := parentBranch.RecordAt(childSlotNum)
			childRightChild := childNode.RightChildPageId()
			childNode.Insert(childNode.NumRecords(), node.NewRecord(nil, parentRecord.KeyBytes(), childRightChild.ToBytes()))

			siblingFirstRecord := siblingNode.RecordAt(0)
			childNode.SetRightChildPageId(page.RestorePageIdFromBytes(siblingFirstRecord.NonKeyBytes()))
			if !parentBranch.Update(childSlotNum, siblingFirstRecord.KeyBytes()) {
				return false, errors.New("failed to update parent branch node key")
			}
			siblingNode.Delete(0)
		}
		return false, nil
	}

	// 兄弟からレコードを転送できない場合はマージする
	if sibling.isLeft {
		// 左の兄弟とマージ: 子(RightChild)のレコードをすべて兄弟(左)に移動、兄弟が残る
		// 親の右端のレコードのキーを下ろして兄弟の末尾に追加
		parentRecord := parentBranch.RecordAt(parentBranch.NumRecords() - 1)
		siblingRightChild := siblingNode.RightChildPageId()
		siblingNode.Insert(siblingNode.NumRecords(), node.NewRecord(nil, parentRecord.KeyBytes(), siblingRightChild.ToBytes()))

		siblingNode.TransferAllFrom(childNode)
		siblingNode.SetRightChildPageId(childNode.RightChildPageId())
		// 親の右端のレコードは不要になるので削除し、RightChild を兄弟ノードに更新
		parentBranch.Delete(parentBranch.NumRecords() - 1)
		parentBranch.SetRightChildPageId(sibling.bufferPage.PageId)
	} else {
		// 右の兄弟とマージ: 兄弟(右)のレコードをすべて子(左)に移動、子が残る
		parentRecord := parentBranch.RecordAt(childSlotNum)
		childRightChild := childNode.RightChildPageId()
		childNode.Insert(childNode.NumRecords(), node.NewRecord(nil, parentRecord.KeyBytes(), childRightChild.ToBytes()))

		childNode.TransferAllFrom(siblingNode)
		childNode.SetRightChildPageId(siblingNode.RightChildPageId())

		if childSlotNum+1 == parentBranch.NumRecords() {
			// 兄弟が RightChild(右端) の場合、親の右端のレコードを削除し、RightChild を子ノードに更新
			parentBranch.Delete(parentBranch.NumRecords() - 1)
			parentBranch.SetRightChildPageId(childBuffer.PageId)
		} else {
			// 兄弟が RightChild(右端) でない場合、キーを更新してから削除
			nextKey := parentBranch.RecordAt(childSlotNum + 1).KeyBytes()
			if !parentBranch.Update(childSlotNum, nextKey) {
				return false, errors.New("failed to update parent branch node key")
			}
			parentBranch.Delete(childSlotNum + 1)
		}
	}

	return !parentBranch.IsHalfFull(), nil
}
