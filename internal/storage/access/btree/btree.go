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

type BTree struct {
	MetaPageId page.PageId
}

// 新しい B+Tree を作成
// 指定された metaPageId を使ってメタページを初期化し、ルートノード (リーフノード) を作成する
func CreateBTree(bpm *bufferpool.BufferPoolManager, metaPageId page.PageId) (*BTree, error) {
	// メタページを初期化
	metaBuf, err := bpm.AddPage(metaPageId)
	if err != nil {
		return nil, err
	}
	meta := metapage.NewMetaPage(metaBuf.GetWriteData())

	// ルートノード (リーフノード) を作成
	rootNodePageId, err := bpm.AllocatePageId(metaPageId.FileId)
	if err != nil {
		return nil, err
	}
	rootBuf, err := bpm.AddPage(rootNodePageId)
	if err != nil {
		return nil, err
	}
	rootLeafNode := node.NewLeafNode(rootBuf.GetWriteData())
	rootLeafNode.Initialize()

	// メタページにルートページIDを設定
	meta.SetRootPageId(rootNodePageId)

	return NewBTree(metaPageId), nil
}

// 既存の B+Tree を開く
func NewBTree(metaPageId page.PageId) *BTree {
	return &BTree{MetaPageId: metaPageId}
}

// 指定された検索モードで B+Tree を検索し、イテレータを返す
// 戻り値: リーフノードのイテレータ
func (bt *BTree) Search(bpm *bufferpool.BufferPoolManager, searchMode SearchMode) (*Iterator, error) {
	rootPage, err := bt.fetchRootPage(bpm)
	if err != nil {
		return nil, err
	}
	return bt.searchRecursively(bpm, rootPage, searchMode)
}

// ルートページを取得
func (bt *BTree) fetchRootPage(bpm *bufferpool.BufferPoolManager) (*bufferpool.BufferPage, error) {
	// メタページを取得
	metaBuf, err := bpm.FetchPage(bt.MetaPageId)
	if err != nil {
		return nil, err
	}
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、 UnRefPage する
	defer bpm.UnRefPage(bt.MetaPageId)
	meta := metapage.NewMetaPage(metaBuf.GetReadData())

	// ルートページを取得
	rootPageId := meta.RootPageId()
	return bpm.FetchPage(rootPageId)
}

// 再起的にノードを辿って該当のリーフノードを見つける
// 戻り値: リーフノードのイテレータ
func (bt *BTree) searchRecursively(bpm *bufferpool.BufferPoolManager, nodeBuffer *bufferpool.BufferPage, searchMode SearchMode) (*Iterator, error) {
	nodeType := node.GetNodeType(nodeBuffer.GetReadData())

	if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		// ブランチノードの場合、再起呼び出し後に UnRefPage を呼び出す (優先的に evict されたいため、不要になったらすぐ UnRefPage する)
		defer bpm.UnRefPage(nodeBuffer.PageId)

		branchNode := node.NewBranchNode(nodeBuffer.GetReadData())

		// 子ノードのページを取得
		childPageId := (func() page.PageId {
			switch sm := searchMode.(type) {
			case SearchModeStart:
				return sm.childPageId(branchNode)
			case SearchModeKey:
				return sm.childPageId(branchNode)
			}
			panic("unreachable") // 実際にはここには到達しないので errors.New ではなく panic で良い
		})()
		childNodePage, err := bpm.FetchPage(childPageId)
		if err != nil {
			return nil, err
		}

		// 再帰呼び出し
		return bt.searchRecursively(bpm, childNodePage, searchMode)
	} else if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
		leafNode := node.NewLeafNode(nodeBuffer.GetReadData())
		slotNum := (func() int {
			switch sm := searchMode.(type) {
			case SearchModeStart:
				return 0
			case SearchModeKey:
				slotNum, _ := leafNode.SearchSlotNum(sm.Key)
				return slotNum
			}
			panic("unreachable") // 実際にはここには到達しないので errors.New ではなく panic で良い
		})()

		iter := newIterator(*nodeBuffer, slotNum)

		// 検索対象のキーが現在のリーフノードの末端のペアより大きい場合、次のリーフノードに進める
		// 例えば、リーフノードに (1, ...), (3, ...), (5, ...) のペアが格納されている場合に、キー 6 を検索したいときなど
		// この場合、次のリーフノードに進めてからイテレータを返す
		if leafNode.NumPairs() == slotNum {
			err := iter.Advance(bpm)
			if err != nil {
				return nil, err
			}
		}

		return iter, nil
	}

	panic("unknown node type") // 実際にはここには到達しないので errors.New ではなく panic で良い
}

// B+Tree にペアを挿入する
func (bt *BTree) Insert(bpm *bufferpool.BufferPoolManager, pair node.Pair) error {
	metaBuf, err := bpm.FetchPage(bt.MetaPageId)
	if err != nil {
		return err
	}
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、UnRefPage する
	defer bpm.UnRefPage(bt.MetaPageId)
	meta := metapage.NewMetaPage(metaBuf.GetReadData())
	rootPageId := meta.RootPageId()
	rootPageBuf, err := bpm.FetchPage(rootPageId)
	if err != nil {
		return err
	}
	defer bpm.UnRefPage(rootPageId)

	overflowKey, overflowChildPageId, err := bt.insertRecursively(bpm, rootPageBuf, pair)
	if err != nil {
		return err
	}
	if overflowChildPageId == nil {
		return nil
	}

	// ルートノードが分割された場合、新しいルートノードを作成する
	newRootPageId, err := bpm.AllocatePageId(bt.MetaPageId.FileId)
	if err != nil {
		return err
	}
	newRootBuf, err := bpm.AddPage(newRootPageId)
	if err != nil {
		return err
	}
	defer bpm.UnRefPage(newRootPageId)
	newRootBranchNode := node.NewBranchNode(newRootBuf.GetWriteData())
	err = newRootBranchNode.Initialize(overflowKey, *overflowChildPageId, rootPageId)
	if err != nil {
		return err
	}
	meta = metapage.NewMetaPage(metaBuf.GetWriteData())
	meta.SetRootPageId(newRootBuf.PageId)
	return nil
}

// 戻り値: (オーバーフローキー, 新しいページ ID, エラー)
func (bt *BTree) insertRecursively(bpm *bufferpool.BufferPoolManager, nodeBuffer *bufferpool.BufferPage, pair node.Pair) ([]byte, *page.PageId, error) {
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
			prevLeafBuffer, err = bpm.FetchPage(*prevLeafPageId)
			if err != nil {
				return nil, nil, err
			}
			defer bpm.UnRefPage(*prevLeafPageId)
		}

		// 新しいリーフノードを作成
		newLeafPageId, err := bpm.AllocatePageId(bt.MetaPageId.FileId)
		if err != nil {
			return nil, nil, err
		}
		newLeafBuffer, err := bpm.AddPage(newLeafPageId)
		if err != nil {
			return nil, nil, err
		}
		defer bpm.UnRefPage(newLeafPageId)

		// 前のリーフノードが存在する場合、そのリーフノードに格納されている nextPageId を、新しいリーフノードのページID に更新する
		if prevLeafBuffer != nil {
			prevLeafNode := node.NewLeafNode(prevLeafBuffer.GetWriteData())
			prevLeafNode.SetNextPageId(&newLeafBuffer.PageId)
		}

		// 新しいリーフノードに分割挿入する
		newLeafNode := node.NewLeafNode(newLeafBuffer.GetWriteData())
		newLeafNode.Initialize()
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
		childNodeBuffer, err := bpm.FetchPage(childPageId)
		if err != nil {
			return nil, nil, err
		}
		defer bpm.UnRefPage(childPageId)

		// 子ノードに対して挿入処理を再起的に実行
		overflowKeyFromChild, overflowChildPageId, err := bt.insertRecursively(bpm, childNodeBuffer, pair)
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
		newBranchPageId, err := bpm.AllocatePageId(bt.MetaPageId.FileId)
		if err != nil {
			return nil, nil, err
		}
		newBranchBuffer, err := bpm.AddPage(newBranchPageId)
		if err != nil {
			return nil, nil, err
		}
		defer bpm.UnRefPage(newBranchPageId)
		newBranchNode := node.NewBranchNode(newBranchBuffer.GetWriteData())
		overflowKey, err := branchNode.SplitInsert(newBranchNode, overFlowPair)
		if err != nil {
			return nil, nil, err
		}

		return overflowKey, &newBranchBuffer.PageId, nil
	}

	panic("unknown node type") // 実際にはここには到達しないので errors.New ではなく panic で良い
}

// B+Tree からペアを削除する
func (bt *BTree) Delete(bpm *bufferpool.BufferPoolManager, key []byte) error {
	metaBuf, err := bpm.FetchPage(bt.MetaPageId)
	if err != nil {
		return err
	}
	defer bpm.UnRefPage(bt.MetaPageId)
	meta := metapage.NewMetaPage(metaBuf.GetReadData())
	rootPageId := meta.RootPageId()
	rootPageBuf, err := bpm.FetchPage(rootPageId)
	if err != nil {
		return err
	}
	defer bpm.UnRefPage(rootPageId)

	_, err = bt.deleteRecursively(bpm, rootPageBuf, key)
	if err != nil {
		return err
	}

	// ルートノードがブランチノードで、子が 1 つになった場合、子をルートにする
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

// deleteResult は削除処理の結果を表す
type deleteResult struct {
	// 子ノードでアンダーフローが発生したかどうか
	underflow bool
}

// 再帰的にノードを辿ってペアを削除する
func (bt *BTree) deleteRecursively(bpm *bufferpool.BufferPoolManager, nodeBuffer *bufferpool.BufferPage, key []byte) (deleteResult, error) {
	nodeType := node.GetNodeType(nodeBuffer.GetReadData())

	if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
		return bt.deleteFromLeaf(nodeBuffer, key)
	} else if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		return bt.deleteFromBranch(bpm, nodeBuffer, key)
	}
	panic("unknown node type") // 実際にはここには到達しないので errors.New ではなく panic で良い
}

// リーフノードからペアを削除する
func (bt *BTree) deleteFromLeaf(nodeBuffer *bufferpool.BufferPage, key []byte) (deleteResult, error) {
	// 削除すべきペア (ペアが格納されているスロット番号) を特定
	leafNode := node.NewLeafNode(nodeBuffer.GetWriteData())
	slotNum, found := leafNode.SearchSlotNum(key)
	if !found {
		return deleteResult{}, ErrKeyNotFound
	}

	// ペアを削除
	leafNode.Delete(slotNum)

	// アンダーフローが発生したかどうかを判定
	underflow := !leafNode.IsHalfFull()

	return deleteResult{underflow: underflow}, nil
}

// 右の兄弟ノードが存在する場合、右の兄弟から借りるかマージする。存在しない場合は左の兄弟から借りるかマージする
type siblingInfo struct {
	pageId     page.PageId
	bufferPage *bufferpool.BufferPage
	// true: 兄弟ノードは左の兄弟ノード, false: 兄弟ノードは右の兄弟ノード
	isLeft bool
}

// ブランチノードから再帰的にペアを削除する
func (bt *BTree) deleteFromBranch(bpm *bufferpool.BufferPoolManager, nodeBuffer *bufferpool.BufferPage, key []byte) (deleteResult, error) {
	branchNode := node.NewBranchNode(nodeBuffer.GetWriteData())
	childIndex := branchNode.SearchChildSlotNum(key)
	childPageId := branchNode.ChildPageIdAt(childIndex)
	childNodeBuffer, err := bpm.FetchPage(childPageId)
	if err != nil {
		return deleteResult{}, err
	}
	defer bpm.UnRefPage(childPageId)

	// 子ノードに対して削除処理を再帰的に実行
	result, err := bt.deleteRecursively(bpm, childNodeBuffer, key)
	if err != nil {
		return deleteResult{}, err
	}

	// 子ノードでアンダーフローが発生しなかった場合、終了
	if !result.underflow {
		return deleteResult{underflow: false}, nil
	}

	// 子ノードでアンダーフローが発生した場合
	sibling := func() siblingInfo {
		if childIndex < branchNode.NumPairs() {
			siblingPageId := branchNode.ChildPageIdAt(childIndex + 1)
			return siblingInfo{pageId: siblingPageId, isLeft: false}
		}
		siblingPageId := branchNode.ChildPageIdAt(childIndex - 1)
		return siblingInfo{pageId: siblingPageId, isLeft: true}
	}()

	siblingBuffer, err := bpm.FetchPage(sibling.pageId)
	if err != nil {
		return deleteResult{}, err
	}
	sibling.bufferPage = siblingBuffer
	defer bpm.UnRefPage(sibling.pageId)

	if bytes.Equal(node.GetNodeType(childNodeBuffer.GetReadData()), node.NODE_TYPE_LEAF) {
		return bt.resolveLeafUnderflow(branchNode, childNodeBuffer, sibling, childIndex), nil
	}
	return bt.resolveBranchUnderflow(branchNode, childNodeBuffer, sibling, childIndex), nil
}

// リーフノードのアンダーフロー処理
// parentBranchNode: 親のブランチノード
// childBuffer: アンダーフローが発生した子ノードのバッファページ (リーフノードのバッファページ)
// sibling: childBuffer と兄弟ノードの情報
// childIndex: childBuffer が親のブランチノードの子ノードの中で何番目か
func (bt *BTree) resolveLeafUnderflow(parentBranchNode *node.BranchNode, childBuffer *bufferpool.BufferPage, sibling siblingInfo, childIndex int) deleteResult {
	childNode := node.NewLeafNode(childBuffer.GetWriteData())
	siblingNode := node.NewLeafNode(sibling.bufferPage.GetWriteData())

	// 兄弟から借りられる場合
	if siblingNode.CanLendPair() {
		// 借りる先の兄弟ノードが左に位置する場合、兄弟の末尾ペアを子の自分のノードの先頭に移動
		if sibling.isLeft {
			lastIndex := siblingNode.NumPairs() - 1
			pair := siblingNode.PairAt(lastIndex)
			childNode.Insert(0, pair)
			siblingNode.Delete(lastIndex)
			parentBranchNode.UpdateKeyAt(childIndex-1, childNode.PairAt(0).Key)
		} else {
			// 借りる先の兄弟ノードが右に位置する場合、兄弟の先頭ペアを子の自分のノードの末尾に移動
			pair := siblingNode.PairAt(0)
			childNode.Insert(childNode.NumPairs(), pair)
			siblingNode.Delete(0)
			parentBranchNode.UpdateKeyAt(childIndex, siblingNode.PairAt(0).Key)
		}
		return deleteResult{underflow: false}
	}

	// 借りられない場合はマージする (左のノードに右のノードをマージ = 左のノードが残る)
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

		if childIndex+1 == parentBranchNode.NumPairs() {
			// 兄弟が RightChild の場合、親の右端のペアを削除し、RightChild を子ノードに更新
			parentBranchNode.Delete(parentBranchNode.NumPairs() - 1)
			parentBranchNode.SetRightChildPageId(childBuffer.PageId)
		} else {
			// 兄弟が RightChild でない場合、キーを更新してから削除
			nextKey := parentBranchNode.PairAt(childIndex + 1).Key
			parentBranchNode.UpdateKeyAt(childIndex, nextKey)
			parentBranchNode.Delete(childIndex + 1) // 右の兄弟を削除するので `childIndex + 1`
		}
	}

	return deleteResult{underflow: !parentBranchNode.IsHalfFull()}
}

// ブランチノードのアンダーフロー処理
// parentBranchNode: 親のブランチノード
// childBuffer: アンダーフローが発生した子ノードのバッファページ (ブランチノードのバッファページ)
// sibling: childBuffer の兄弟ノードの情報
// childIndex: childBuffer が親のブランチノードの子ノードの中で何番目か
func (bt *BTree) resolveBranchUnderflow(parentBranchNode *node.BranchNode, childBuffer *bufferpool.BufferPage, sibling siblingInfo, childIndex int) deleteResult {
	childNode := node.NewBranchNode(childBuffer.GetWriteData())
	siblingNode := node.NewBranchNode(sibling.bufferPage.GetWriteData())

	// 兄弟から借りられる場合
	if siblingNode.CanLendPair() {
		if sibling.isLeft {
			// 左の兄弟から借りる: 親の境界キーを子の先頭に下ろし、兄弟の末尾キーを親に上げる
			parentPair := parentBranchNode.PairAt(childIndex - 1)
			siblingRightChild := siblingNode.RightChildPageId()
			childNode.Insert(0, node.NewPair(parentPair.Key, siblingRightChild.ToBytes()))

			lastIndex := siblingNode.NumPairs() - 1
			siblingLastPair := siblingNode.PairAt(lastIndex)
			parentBranchNode.UpdateKeyAt(childIndex-1, siblingLastPair.Key)
			siblingNode.SetRightChildPageId(page.PageIdFromBytes(siblingLastPair.Value))
			siblingNode.Delete(lastIndex)
		} else {
			// 右の兄弟から借りる: 親の境界キーを子の末尾に下ろし、兄弟の先頭キーを親に上げる
			parentPair := parentBranchNode.PairAt(childIndex)
			childRightChild := childNode.RightChildPageId()
			childNode.Insert(childNode.NumPairs(), node.NewPair(parentPair.Key, childRightChild.ToBytes()))

			siblingFirstPair := siblingNode.PairAt(0)
			childNode.SetRightChildPageId(page.PageIdFromBytes(siblingFirstPair.Value))
			parentBranchNode.UpdateKeyAt(childIndex, siblingFirstPair.Key)
			siblingNode.Delete(0)
		}
		return deleteResult{underflow: false}
	}

	// 借りられない場合はマージする (左のノードに右のノードをマージ = 左のノードが残る)
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
		parentPair := parentBranchNode.PairAt(childIndex)
		childRightChild := childNode.RightChildPageId()
		childNode.Insert(childNode.NumPairs(), node.NewPair(parentPair.Key, childRightChild.ToBytes()))

		childNode.TransferAllFrom(siblingNode)
		childNode.SetRightChildPageId(siblingNode.RightChildPageId())

		if childIndex+1 == parentBranchNode.NumPairs() {
			// 兄弟が RightChild の場合、親の右端のペアを削除し、RightChild を子ノードに更新
			parentBranchNode.Delete(parentBranchNode.NumPairs() - 1)
			parentBranchNode.SetRightChildPageId(childBuffer.PageId)
		} else {
			// 兄弟が RightChild でない場合、キーを更新してから削除
			nextKey := parentBranchNode.PairAt(childIndex + 1).Key
			parentBranchNode.UpdateKeyAt(childIndex, nextKey)
			parentBranchNode.Delete(childIndex + 1)
		}
	}

	return deleteResult{underflow: !parentBranchNode.IsHalfFull()}
}
