package btree

import (
	"bytes"
	"errors"
	metapage "minesql/internal/storage/access/btree/meta_page"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/page"
)

var (
	ErrDuplicateKey = errors.New("duplicate key")
	ErrKeyNotFound  = errors.New("key not found")
)

type BPlusTree struct {
	MetaPageId page.PageId
}

// CreateBPlusTree は新しい B+Tree を作成
// 指定された metaPageId を使ってメタページを初期化し、ルートノード (リーフノード) を作成する
func CreateBPlusTree(bp *bufferpool.BufferPool, metaPageId page.PageId) (*BPlusTree, error) {
	// メタページを初期化
	metaBuf, err := bp.AddPage(metaPageId)
	if err != nil {
		return nil, err
	}
	meta := metapage.NewMetaPage(metaBuf.GetWriteData())

	// ルートノード (リーフノード) を作成
	rootNodePageId, err := bp.AllocatePageId(metaPageId.FileId)
	if err != nil {
		return nil, err
	}
	rootBuf, err := bp.AddPage(rootNodePageId)
	if err != nil {
		return nil, err
	}
	rootLeafNode := node.NewLeafNode(rootBuf.GetWriteData())
	rootLeafNode.Initialize()

	// メタページにルートページIDを設定
	meta.SetRootPageId(rootNodePageId)

	return NewBPlusTree(metaPageId), nil
}

// NewBPlusTree は既存の B+Tree を開く
func NewBPlusTree(metaPageId page.PageId) *BPlusTree {
	return &BPlusTree{MetaPageId: metaPageId}
}

// ==================================
// Search
// ==================================

// Search は指定された検索モードで B+Tree を検索し、イテレータを返す
//
// 戻り値: リーフノードのイテレータ
func (bt *BPlusTree) Search(bp *bufferpool.BufferPool, searchMode SearchMode) (*Iterator, error) {
	// メタページを取得
	metaBuf, err := bp.FetchPage(bt.MetaPageId)
	if err != nil {
		return nil, err
	}
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、 UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	meta := metapage.NewMetaPage(metaBuf.GetReadData())

	// ルートページを取得
	rootPageId := meta.RootPageId()
	rootPage, err := bp.FetchPage(rootPageId)
	if err != nil {
		return nil, err
	}

	return bt.searchRecursively(bp, rootPage, searchMode)
}

// searchRecursively は再起的にノードを辿って該当のリーフノードを見つける
//
// 戻り値: リーフノードのイテレータ
func (bt *BPlusTree) searchRecursively(bp *bufferpool.BufferPool, nodeBuffer *bufferpool.BufferPage, searchMode SearchMode) (*Iterator, error) {
	nodeType := node.GetNodeType(nodeBuffer.GetReadData())

	if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		// ブランチノードの場合、再起呼び出し後に UnRefPage を呼び出す (優先的に evict されたいため、不要になったらすぐ UnRefPage する)
		defer bp.UnRefPage(nodeBuffer.PageId)

		branchNode := node.NewBranchNode(nodeBuffer.GetReadData())

		// 子ノードのページを取得
		childPageId := searchMode.childPageId(branchNode)
		childNodePage, err := bp.FetchPage(childPageId)
		if err != nil {
			return nil, err
		}

		// 再帰呼び出し
		return bt.searchRecursively(bp, childNodePage, searchMode)
	} else if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
		leafNode := node.NewLeafNode(nodeBuffer.GetReadData())

		switch sm := searchMode.(type) {
		case SearchModeStart:
			return newIterator(*nodeBuffer, 0), nil
		case SearchModeKey:
			slotNum, _ := leafNode.SearchSlotNum(sm.Key)
			iter := newIterator(*nodeBuffer, slotNum)
			// 検索対象のキーが現在のリーフノードの末端のペアより大きい場合、次のリーフノードに進める
			// 例: リーフノードに (1, ...), (3, ...), (5, ...) のペアが格納されている場合に、キー 6 を検索したいときなど
			// (この場合 `leafNode.SearchSlotNum(sm.Key)` は `leafNode.NumPairs()` と等しい値を返す)
			// この場合、次のリーフノードに進めてからイテレータを返す
			if leafNode.NumPairs() == slotNum {
				err := iter.Advance(bp)
				if err != nil {
					return nil, err
				}
			}
			return iter, nil
		}
	}

	panic("unknown node type") // 実際にはここには到達しないので errors.New ではなく panic で良い
}

// ==================================
// Insert
// ==================================

// Insert は B+Tree にペアを挿入する
func (bt *BPlusTree) Insert(bp *bufferpool.BufferPool, pair node.Pair) error {
	// メタページを取得
	metaBuf, err := bp.FetchPage(bt.MetaPageId)
	if err != nil {
		return err
	}
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	meta := metapage.NewMetaPage(metaBuf.GetReadData())

	// ルートページを取得
	rootPageId := meta.RootPageId()
	rootPageBuf, err := bp.FetchPage(rootPageId)
	if err != nil {
		return err
	}

	overflowKey, overflowChildPageId, err := bt.insertRecursively(bp, rootPageBuf, pair)
	if err != nil {
		return err
	}

	// ルートノードの分割が発生しなかった場合、終了
	if overflowChildPageId == nil {
		return nil
	}

	// ルートノードが分割された場合、新しいルートノードを作成する
	newRootPageId, err := bp.AllocatePageId(bt.MetaPageId.FileId)
	if err != nil {
		return err
	}
	newRootBuf, err := bp.AddPage(newRootPageId)
	if err != nil {
		return err
	}

	newRootBranchNode := node.NewBranchNode(newRootBuf.GetWriteData())
	err = newRootBranchNode.Initialize(overflowKey, *overflowChildPageId, rootPageId)
	if err != nil {
		return err
	}

	// メタページに新しいルートページIDを設定する
	meta = metapage.NewMetaPage(metaBuf.GetWriteData())
	meta.SetRootPageId(newRootBuf.PageId)
	return nil
}

// insertRecursively は再帰的にノードを辿ってペアを挿入する
//
// 戻り値: (オーバーフローキー, 新しいページ ID, エラー)
//
// 挿入に際しノード分割が発生した場合、オーバーフローキーは分割後に親ノードに伝播させる境界キーになり、新しいページ ID は分割してできた新しいノードのページ ID になる
//
// ※分割挿入発生時にできる新しいノードは、分割元の前に位置するノードとして作られる
func (bt *BPlusTree) insertRecursively(bp *bufferpool.BufferPool, nodeBuffer *bufferpool.BufferPage, pair node.Pair) ([]byte, *page.PageId, error) {
	nodeType := node.GetNodeType(nodeBuffer.GetReadData())

	if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
		leafNode := node.NewLeafNode(nodeBuffer.GetWriteData())
		slotNum, found := leafNode.SearchSlotNum(pair.Key)
		if found {
			return nil, nil, ErrDuplicateKey
		}

		// リーフノードに挿入できた場合、終了
		if leafNode.Insert(slotNum, pair) {
			return nil, nil, nil
		}

		// リーフノードが満杯の場合、分割する
		prevLeafPageId := leafNode.PrevPageId()
		var prevLeafBuffer *bufferpool.BufferPage
		var err error

		// 前のリーフノードが存在する場合、そのページを取得
		if prevLeafPageId != nil {
			prevLeafBuffer, err = bp.FetchPage(*prevLeafPageId)
			if err != nil {
				return nil, nil, err
			}
			defer bp.UnRefPage(*prevLeafPageId)
		}

		// 新しいリーフノードを作成
		newLeafPageId, err := bp.AllocatePageId(bt.MetaPageId.FileId)
		if err != nil {
			return nil, nil, err
		}
		newLeafBuffer, err := bp.AddPage(newLeafPageId)
		if err != nil {
			return nil, nil, err
		}
		defer bp.UnRefPage(newLeafPageId)

		// 前のリーフノードが存在する場合、そのリーフノードに格納されている nextPageId を、新しいリーフノードのページID に更新する
		if prevLeafBuffer != nil {
			prevLeafNode := node.NewLeafNode(prevLeafBuffer.GetWriteData())
			prevLeafNode.SetNextPageId(&newLeafBuffer.PageId)
		}

		// 新しいリーフノードに分割挿入する
		newLeafNode := node.NewLeafNode(newLeafBuffer.GetWriteData())
		_, err = leafNode.SplitInsert(newLeafNode, pair)
		if err != nil {
			return nil, nil, err
		}
		newLeafNode.SetNextPageId(&nodeBuffer.PageId)
		newLeafNode.SetPrevPageId(prevLeafPageId)
		leafNode.SetPrevPageId(&newLeafBuffer.PageId)

		// overflowKey は古いリーフノードの先頭のキー (親ノードの境界キーになる)
		overflowKey := leafNode.PairAt(0).Key
		return overflowKey, &newLeafBuffer.PageId, nil
	} else if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		// 挿入先の子ノードを取得
		branchNode := node.NewBranchNode(nodeBuffer.GetWriteData())
		childIndex := branchNode.SearchChildSlotNum(pair.Key)
		childPageId := branchNode.ChildPageIdAt(childIndex)
		childNodeBuffer, err := bp.FetchPage(childPageId)
		if err != nil {
			return nil, nil, err
		}
		defer bp.UnRefPage(childPageId)

		// 子ノードに対して挿入処理を再起的に実行
		overflowKeyFromChild, overflowChildPageId, err := bt.insertRecursively(bp, childNodeBuffer, pair)
		if err != nil {
			return nil, nil, err
		}

		// 子ノードが分割されなかった場合、終了
		if overflowChildPageId == nil {
			return nil, nil, nil
		}

		// 子ノードが分割された場合、子ノードから返されたキーとページIDをペアとして、ブランチノードに挿入
		overFlowPair := node.NewPair(overflowKeyFromChild, overflowChildPageId.ToBytes())
		if branchNode.Insert(childIndex, overFlowPair) {
			return nil, nil, nil
		}

		// ブランチノードが満杯で挿入に失敗した場合、分割する
		newBranchPageId, err := bp.AllocatePageId(bt.MetaPageId.FileId)
		if err != nil {
			return nil, nil, err
		}
		newBranchBuffer, err := bp.AddPage(newBranchPageId)
		if err != nil {
			return nil, nil, err
		}
		defer bp.UnRefPage(newBranchPageId)
		newBranchNode := node.NewBranchNode(newBranchBuffer.GetWriteData())
		overflowKey, err := branchNode.SplitInsert(newBranchNode, overFlowPair)
		if err != nil {
			return nil, nil, err
		}

		return overflowKey, &newBranchBuffer.PageId, nil
	}

	panic("unknown node type") // 実際にはここには到達しないので errors.New ではなく panic で良い
}

// ==================================
// Delete
// =================================

// Delete は B+Tree からペアを削除する
func (bt *BPlusTree) Delete(bp *bufferpool.BufferPool, key []byte) error {
	// メタページを取得
	metaBuf, err := bp.FetchPage(bt.MetaPageId)
	if err != nil {
		return err
	}
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	meta := metapage.NewMetaPage(metaBuf.GetReadData())

	// ルートページを取得
	rootPageId := meta.RootPageId()
	rootPageBuf, err := bp.FetchPage(rootPageId)
	if err != nil {
		return err
	}

	_, err = bt.deleteRecursively(bp, rootPageBuf, key)
	if err != nil {
		return err
	}

	// ルートノードがブランチノードで、子が 1 つになった場合 (=ブランチノード1, リーフノード1 になった場合)、子をルートにする
	nodeType := node.GetNodeType(rootPageBuf.GetReadData())
	if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		branchNode := node.NewBranchNode(rootPageBuf.GetReadData())
		if branchNode.NumPairs() == 0 {
			// 子が 1 つになった場合、右端の子をルートにする
			newRootPageId := branchNode.ChildPageIdAt(0)
			meta = metapage.NewMetaPage(metaBuf.GetWriteData())
			meta.SetRootPageId(newRootPageId)
		}
	}

	return nil
}

// deleteRecursively は再帰的にノードを辿ってペアを削除する
//
// 戻り値: (アンダーフローが発生したかどうか, エラー)
func (bt *BPlusTree) deleteRecursively(bp *bufferpool.BufferPool, nodeBuffer *bufferpool.BufferPage, key []byte) (underflow bool, err error) {
	nodeType := node.GetNodeType(nodeBuffer.GetReadData())

	if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
		return bt.deleteFromLeaf(nodeBuffer, key)
	} else if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		return bt.deleteFromBranch(bp, nodeBuffer, key)
	}
	panic("unknown node type") // 実際にはここには到達しないので errors.New ではなく panic で良い
}

// siblingInfo は兄弟ノードの情報を表す
type siblingInfo struct {
	pageId     page.PageId
	bufferPage *bufferpool.BufferPage
	isLeft     bool // true: 兄弟ノードは左の兄弟ノード, false: 兄弟ノードは右の兄弟ノード
}

// deleteFromBranch はブランチノードから再帰的にペアを削除する
//
// bp: バッファプール
//
// nodeBuffer: 削除処理を行うノードのバッファページ (ブランチノードのバッファページ)
//
// key: 削除するキー
//
// 戻り値: (アンダーフローが発生したかどうか, エラー)
func (bt *BPlusTree) deleteFromBranch(bp *bufferpool.BufferPool, nodeBuffer *bufferpool.BufferPage, key []byte) (underflow bool, err error) {
	branchNode := node.NewBranchNode(nodeBuffer.GetWriteData())
	childSlotNum := branchNode.SearchChildSlotNum(key)
	childPageId := branchNode.ChildPageIdAt(childSlotNum)
	childNodeBuffer, err := bp.FetchPage(childPageId)
	if err != nil {
		return false, err
	}
	defer bp.UnRefPage(childPageId)

	// 子ノードに対して削除処理を再帰的に実行
	underflow, err = bt.deleteRecursively(bp, childNodeBuffer, key)
	if err != nil {
		return false, err
	}

	// 子ノードでアンダーフローが発生しなかった場合
	if !underflow {
		return false, nil
	}

	// 子ノードでアンダーフローが発生した場合

	// 転送・マージする兄弟ノードを決定
	sibling := func() siblingInfo {
		if childSlotNum < branchNode.NumPairs() {
			siblingPageId := branchNode.ChildPageIdAt(childSlotNum + 1)
			return siblingInfo{pageId: siblingPageId, isLeft: false}
		}
		siblingPageId := branchNode.ChildPageIdAt(childSlotNum - 1)
		return siblingInfo{pageId: siblingPageId, isLeft: true}
	}()

	siblingBuffer, err := bp.FetchPage(sibling.pageId)
	if err != nil {
		return false, err
	}
	sibling.bufferPage = siblingBuffer
	defer bp.UnRefPage(sibling.pageId)

	if bytes.Equal(node.GetNodeType(childNodeBuffer.GetReadData()), node.NODE_TYPE_LEAF) {
		return bt.resolveLeafUnderflow(branchNode, childNodeBuffer, sibling, childSlotNum)
	}
	return bt.resolveBranchUnderflow(branchNode, childNodeBuffer, sibling, childSlotNum)
}

// deleteFromLeaf はリーフノードからペアを削除する
//
// 戻り値: (アンダーフローが発生したかどうか, エラー)
func (bt *BPlusTree) deleteFromLeaf(nodeBuffer *bufferpool.BufferPage, key []byte) (underflow bool, err error) {
	// 削除すべきペア (ペアが格納されているスロット番号) を特定
	leafNode := node.NewLeafNode(nodeBuffer.GetWriteData())
	slotNum, found := leafNode.SearchSlotNum(key)
	if !found {
		return false, ErrKeyNotFound
	}

	// ペアを削除
	leafNode.Delete(slotNum)

	// アンダーフローが発生したかどうかを判定
	return !leafNode.IsHalfFull(), nil
}

// resolveLeafUnderflow　はリーフノードのアンダーフロー処理を行う
//
// parentBranchNode: 親のブランチノード
//
// childBuffer: アンダーフローが発生した子ノードのバッファページ (リーフノードのバッファページ)
//
// sibling: childBuffer と兄弟ノードの情報
//
// childSlotNum: childBuffer が親のブランチノードの子ノードの中で何番目か
func (bt *BPlusTree) resolveLeafUnderflow(parentBranchNode *node.BranchNode, childBuffer *bufferpool.BufferPage, sibling siblingInfo, childSlotNum int) (underflow bool, err error) {
	childNode := node.NewLeafNode(childBuffer.GetWriteData())
	siblingNode := node.NewLeafNode(sibling.bufferPage.GetWriteData())

	// 兄弟からペアを転送できる場合
	if siblingNode.CanTransferPair(sibling.isLeft) {
		if sibling.isLeft {
			// 左の兄弟から転送: 左の兄弟の末尾ペアを先頭に移動
			lastIndex := siblingNode.NumPairs() - 1
			pair := siblingNode.PairAt(lastIndex)
			childNode.Insert(0, pair)
			siblingNode.Delete(lastIndex)
			if !parentBranchNode.Update(childSlotNum-1, childNode.PairAt(0).Key) {
				return false, errors.New("failed to update parent branch node key")
			}
		} else {
			// 右の兄弟から転送: 右の兄弟の先頭ペアを末尾に移動
			pair := siblingNode.PairAt(0)
			childNode.Insert(childNode.NumPairs(), pair)
			siblingNode.Delete(0)
			if !parentBranchNode.Update(childSlotNum, siblingNode.PairAt(0).Key) {
				return false, errors.New("failed to update parent branch node key")
			}
		}
		return false, nil
	}

	// 兄弟からペアを転送できない場合はマージする
	if sibling.isLeft {
		// 左の兄弟とマージ: 子(RightChild)のペアをすべて兄弟(左)に移動、兄弟が残る
		siblingNode.TransferAllFrom(childNode)
		siblingNode.SetNextPageId(childNode.NextPageId())
		// 親の右端のペアは不要になるので削除し、RightChild を兄弟ノードに更新
		parentBranchNode.Delete(parentBranchNode.NumPairs() - 1)
		parentBranchNode.SetRightChildPageId(sibling.bufferPage.PageId)
	} else {
		// 右の兄弟とマージ: 兄弟(右)のペアをすべて子(左)に移動、子が残る
		childNode.TransferAllFrom(siblingNode)
		childNode.SetNextPageId(siblingNode.NextPageId())

		if childSlotNum+1 == parentBranchNode.NumPairs() {
			// 兄弟が RightChild(右端) の場合、親の右端のペアを削除し、RightChild を子ノードに更新
			parentBranchNode.Delete(parentBranchNode.NumPairs() - 1)
			parentBranchNode.SetRightChildPageId(childBuffer.PageId)
		} else {
			// 兄弟が RightChild(右端) でない場合、キーを更新してから削除
			nextKey := parentBranchNode.PairAt(childSlotNum + 1).Key
			if !parentBranchNode.Update(childSlotNum, nextKey) {
				return false, errors.New("failed to update parent branch node key")
			}
			parentBranchNode.Delete(childSlotNum + 1) // 右の兄弟を削除するので `childIndex + 1`
		}
	}

	return !parentBranchNode.IsHalfFull(), nil
}

// resolveBranchUnderflow はブランチノードのアンダーフロー処理を行う
//
// parentBranchNode: 親のブランチノード
//
// childBuffer: アンダーフローが発生した子ノードのバッファページ (ブランチノードのバッファページ)
//
// sibling: childBuffer の兄弟ノードの情報
//
// childSlotNum: childBuffer が親のブランチノードの子ノードの中で何番目か
func (bt *BPlusTree) resolveBranchUnderflow(parentBranchNode *node.BranchNode, childBuffer *bufferpool.BufferPage, sibling siblingInfo, childSlotNum int) (underflow bool, err error) {
	childNode := node.NewBranchNode(childBuffer.GetWriteData())
	siblingNode := node.NewBranchNode(sibling.bufferPage.GetWriteData())

	// 兄弟からペアを転送できる場合
	if siblingNode.CanTransferPair(sibling.isLeft) {
		if sibling.isLeft {
			// 左の兄弟から転送: 親の境界キーを子の先頭に下ろし、兄弟の末尾キーを親に上げる
			parentPair := parentBranchNode.PairAt(childSlotNum - 1)
			siblingRightChild := siblingNode.RightChildPageId()
			childNode.Insert(0, node.NewPair(parentPair.Key, siblingRightChild.ToBytes()))

			lastIndex := siblingNode.NumPairs() - 1
			siblingLastPair := siblingNode.PairAt(lastIndex)
			if !parentBranchNode.Update(childSlotNum-1, siblingLastPair.Key) {
				return false, errors.New("failed to update parent branch node key")
			}
			siblingNode.SetRightChildPageId(page.RestorePageIdFromBytes(siblingLastPair.Value))
			siblingNode.Delete(lastIndex)
		} else {
			// 右の兄弟から転送: 親の境界キーを子の末尾に下ろし、兄弟の先頭キーを親に上げる
			parentPair := parentBranchNode.PairAt(childSlotNum)
			childRightChild := childNode.RightChildPageId()
			childNode.Insert(childNode.NumPairs(), node.NewPair(parentPair.Key, childRightChild.ToBytes()))

			siblingFirstPair := siblingNode.PairAt(0)
			childNode.SetRightChildPageId(page.RestorePageIdFromBytes(siblingFirstPair.Value))
			if !parentBranchNode.Update(childSlotNum, siblingFirstPair.Key) {
				return false, errors.New("failed to update parent branch node key")
			}
			siblingNode.Delete(0)
		}
		return false, nil
	}

	// 兄弟からペアを転送できない場合はマージする
	if sibling.isLeft {
		// 左の兄弟とマージ: 子(RightChild)のペアをすべて兄弟(左)に移動、兄弟が残る
		// 親の右端のペアのキーを下ろして兄弟の末尾に追加
		parentPair := parentBranchNode.PairAt(parentBranchNode.NumPairs() - 1)
		siblingRightChild := siblingNode.RightChildPageId()
		siblingNode.Insert(siblingNode.NumPairs(), node.NewPair(parentPair.Key, siblingRightChild.ToBytes()))

		siblingNode.TransferAllFrom(childNode)
		siblingNode.SetRightChildPageId(childNode.RightChildPageId())
		// 親の右端のペアは不要になるので削除し、RightChild を兄弟ノードに更新
		parentBranchNode.Delete(parentBranchNode.NumPairs() - 1)
		parentBranchNode.SetRightChildPageId(sibling.bufferPage.PageId)
	} else {
		// 右の兄弟とマージ: 兄弟(右)のペアをすべて子(左)に移動、子が残る
		parentPair := parentBranchNode.PairAt(childSlotNum)
		childRightChild := childNode.RightChildPageId()
		childNode.Insert(childNode.NumPairs(), node.NewPair(parentPair.Key, childRightChild.ToBytes()))

		childNode.TransferAllFrom(siblingNode)
		childNode.SetRightChildPageId(siblingNode.RightChildPageId())

		if childSlotNum+1 == parentBranchNode.NumPairs() {
			// 兄弟が RightChild(右端) の場合、親の右端のペアを削除し、RightChild を子ノードに更新
			parentBranchNode.Delete(parentBranchNode.NumPairs() - 1)
			parentBranchNode.SetRightChildPageId(childBuffer.PageId)
		} else {
			// 兄弟が RightChild(右端) でない場合、キーを更新してから削除
			nextKey := parentBranchNode.PairAt(childSlotNum + 1).Key
			if !parentBranchNode.Update(childSlotNum, nextKey) {
				return false, errors.New("failed to update parent branch node key")
			}
			parentBranchNode.Delete(childSlotNum + 1)
		}
	}

	return !parentBranchNode.IsHalfFull(), nil
}

// ==================================
// Update
// ==================================

// Update は B+Tree の特定のノードの値 (value) を更新する
//
// pair.Key で対象のリーフノードを特定し、pair.Value で値を上書きする
func (bt *BPlusTree) Update(bp *bufferpool.BufferPool, pair node.Pair) error {
	// メタページを取得
	metaBuf, err := bp.FetchPage(bt.MetaPageId)
	if err != nil {
		return err
	}
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	meta := metapage.NewMetaPage(metaBuf.GetReadData())

	// ルートページを取得
	rootPageId := meta.RootPageId()
	rootPageBuf, err := bp.FetchPage(rootPageId)
	if err != nil {
		return err
	}

	return bt.updateRecursively(bp, rootPageBuf, pair)
}

// updateRecursively は再帰的にノードを辿って該当のリーフノードを見つけ、ペアを更新する
func (bt *BPlusTree) updateRecursively(bp *bufferpool.BufferPool, nodeBuffer *bufferpool.BufferPage, pair node.Pair) error {
	nodeType := node.GetNodeType(nodeBuffer.GetReadData())

	if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		// ブランチノードの場合、再帰呼び出し後に UnRefPage を呼び出す
		defer bp.UnRefPage(nodeBuffer.PageId)

		branchNode := node.NewBranchNode(nodeBuffer.GetReadData())

		// pair.Key を使って子ノードを特定
		searchMode := SearchModeKey{Key: pair.Key}
		childPageId := searchMode.childPageId(branchNode)
		childNodeBuffer, err := bp.FetchPage(childPageId)
		if err != nil {
			return err
		}

		// 再帰呼び出し
		return bt.updateRecursively(bp, childNodeBuffer, pair)
	} else if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
		leafNode := node.NewLeafNode(nodeBuffer.GetWriteData())

		// 該当のキーを持つペアを見つける
		slotNum, found := leafNode.SearchSlotNum(pair.Key)
		if !found {
			return ErrKeyNotFound
		}

		// ペアの value を新しい値に更新
		if !leafNode.Update(slotNum, pair) {
			return errors.New("failed to update pair")
		}
		return nil
	}

	panic("unknown node type") // 実際にはここには到達しないので errors.New ではなく panic で良い
}
