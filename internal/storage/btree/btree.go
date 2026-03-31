package btree

import (
	"bytes"
	"errors"
	"minesql/internal/storage/btree/node"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/page"
)

var (
	ErrDuplicateKey = errors.New("duplicate key")
	ErrKeyNotFound  = errors.New("key not found")
)

type BTree struct {
	MetaPageId page.PageId
}

// CreateBTree は新しい B+Tree を作成
// 指定された metaPageId を使ってメタページを初期化し、ルートノード (リーフノード) を作成する
func CreateBTree(bp *buffer.BufferPool, metaPageId page.PageId) (*BTree, error) {
	// メタページを初期化
	metaBuf, err := bp.AddPage(metaPageId)
	if err != nil {
		return nil, err
	}
	meta := newMetaPage(metaBuf.GetWriteData())

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

	// メタページにルートページID・リーフページ数・高さを設定
	meta.setRootPageId(rootNodePageId)
	meta.setLeafPageCount(1) // 初期状態はルートリーフノード 1 つ
	meta.setHeight(1)        // ルートリーフノードのみなので高さ 1

	return NewBTree(metaPageId), nil
}

// NewBTree は既存の B+Tree を開く
func NewBTree(metaPageId page.PageId) *BTree {
	return &BTree{MetaPageId: metaPageId}
}

// ==================================
// Search
// ==================================

// Search は指定された検索モードで B+Tree を検索し、イテレータを返す
//
// 戻り値: リーフノードのイテレータ
func (bt *BTree) Search(bp *buffer.BufferPool, searchMode SearchMode) (*Iterator, error) {
	// メタページを取得
	metaBuf, err := bp.FetchPage(bt.MetaPageId)
	if err != nil {
		return nil, err
	}
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、 UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	meta := newMetaPage(metaBuf.GetReadData())

	// ルートページを取得
	rootPageId := meta.rootPageId()
	rootPage, err := bp.FetchPage(rootPageId)
	if err != nil {
		return nil, err
	}

	return bt.searchRecursively(bp, rootPage, searchMode)
}

// searchRecursively は再起的にノードを辿って該当のリーフノードを見つける
//
// 戻り値: リーフノードのイテレータ
func (bt *BTree) searchRecursively(bp *buffer.BufferPool, nodeBuffer *buffer.BufferPage, searchMode SearchMode) (*Iterator, error) {
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
			// 検索対象のキーが現在のリーフノードの末端のレコードより大きい場合、次のリーフノードに進める
			// 例: リーフノードに (1, ...), (3, ...), (5, ...) のレコードが格納されている場合に、キー 6 を検索したいときなど
			// (この場合 `leafNode.SearchSlotNum(sm.Key)` は `leafNode.NumRecords()` と等しい値を返す)
			// この場合、次のリーフノードに進めてからイテレータを返す
			if leafNode.NumRecords() == slotNum {
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

// FindByKey は指定されたキーで B+Tree を検索し、完全一致するレコードを返す
//
// キーが見つからない場合は ErrKeyNotFound を返す
func (bt *BTree) FindByKey(bp *buffer.BufferPool, key []byte) (node.Record, error) {
	iter, err := bt.Search(bp, SearchModeKey{Key: key})
	if err != nil {
		return nil, err
	}
	record, ok := iter.Get()
	if !ok {
		return nil, ErrKeyNotFound
	}
	if !bytes.Equal(record.KeyBytes(), key) {
		return nil, ErrKeyNotFound
	}
	return record, nil
}

// ==================================
// Insert
// ==================================

// Insert は B+Tree にレコードを挿入する
func (bt *BTree) Insert(bp *buffer.BufferPool, record node.Record) error {
	// メタページを取得
	metaBuf, err := bp.FetchPage(bt.MetaPageId)
	if err != nil {
		return err
	}
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	meta := newMetaPage(metaBuf.GetReadData())

	// ルートページを取得
	rootPageId := meta.rootPageId()
	rootPageBuf, err := bp.FetchPage(rootPageId)
	if err != nil {
		return err
	}

	overflowKey, overflowChildPageId, leafSplit, err := bt.insertRecursively(bp, rootPageBuf, record)
	if err != nil {
		return err
	}

	rootSplit := !overflowChildPageId.IsInvalid()

	// リーフノードの分割もルートノードの分割も発生しなかった場合、終了
	if !leafSplit && !rootSplit {
		return nil
	}

	// リーフノード・ルートノードの分割が発生した場合 (この二つは同時に発生する可能性もある)
	meta = newMetaPage(metaBuf.GetWriteData())

	// リーフ分割が発生した場合、メタページのリーフページ数を更新
	if leafSplit {
		meta.setLeafPageCount(meta.leafPageCount() + 1)
	}

	// ルートノードの分割が発生した場合、新しいルートノードを作成してメタページを更新
	if rootSplit {
		newRootPageId, err := bp.AllocatePageId(bt.MetaPageId.FileId)
		if err != nil {
			return err
		}
		newRootBuf, err := bp.AddPage(newRootPageId)
		if err != nil {
			return err
		}
		newRootBranchNode := node.NewBranchNode(newRootBuf.GetWriteData())
		err = newRootBranchNode.Initialize(overflowKey, overflowChildPageId, rootPageId)
		if err != nil {
			return err
		}

		meta.setRootPageId(newRootBuf.PageId)
		meta.setHeight(meta.height() + 1)
	}

	return nil
}

// insertRecursively は再帰的にノードを辿ってレコードを挿入する
//
// 戻り値: (オーバーフローキー, 新しいページ ID, リーフ分割が発生したか, エラー)
//
// ノード分割が発生しなかった場合、新しいページ ID には INVALID_PAGE_ID が返る。
// 分割が発生した場合、オーバーフローキーは親ノードに伝播させる境界キーになり、新しいページ ID は分割で作られたノードのページ ID になる。
//
// ※分割挿入発生時にできる新しいノードは、分割元の前に位置するノードとして作られる
func (bt *BTree) insertRecursively(bp *buffer.BufferPool, nodeBuffer *buffer.BufferPage, record node.Record) ([]byte, page.PageId, bool, error) {
	nodeType := node.GetNodeType(nodeBuffer.GetReadData())

	if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
		leafNode := node.NewLeafNode(nodeBuffer.GetWriteData())
		slotNum, found := leafNode.SearchSlotNum(record.KeyBytes())
		if found {
			return nil, page.INVALID_PAGE_ID, false, ErrDuplicateKey
		}

		// リーフノードに挿入できた場合、終了
		if leafNode.Insert(slotNum, record) {
			return nil, page.INVALID_PAGE_ID, false, nil
		}

		// リーフノードが満杯の場合、分割する
		prevLeafPageId := leafNode.PrevPageId()
		var prevLeafBuffer *buffer.BufferPage
		var err error

		// 前のリーフノードが存在する場合、そのページを取得
		if prevLeafPageId != nil {
			prevLeafBuffer, err = bp.FetchPage(*prevLeafPageId)
			if err != nil {
				return nil, page.INVALID_PAGE_ID, false, err
			}
			defer bp.UnRefPage(*prevLeafPageId)
		}

		// 新しいリーフノードを作成
		newLeafPageId, err := bp.AllocatePageId(bt.MetaPageId.FileId)
		if err != nil {
			return nil, page.INVALID_PAGE_ID, false, err
		}
		newLeafBuffer, err := bp.AddPage(newLeafPageId)
		if err != nil {
			return nil, page.INVALID_PAGE_ID, false, err
		}
		defer bp.UnRefPage(newLeafPageId)

		// 前のリーフノードが存在する場合、そのリーフノードに格納されている nextPageId を、新しいリーフノードのページID に更新する
		if prevLeafBuffer != nil {
			prevLeafNode := node.NewLeafNode(prevLeafBuffer.GetWriteData())
			prevLeafNode.SetNextPageId(&newLeafBuffer.PageId)
		}

		// 新しいリーフノードに分割挿入する
		newLeafNode := node.NewLeafNode(newLeafBuffer.GetWriteData())
		_, err = leafNode.SplitInsert(newLeafNode, record)
		if err != nil {
			return nil, page.INVALID_PAGE_ID, false, err
		}
		newLeafNode.SetNextPageId(&nodeBuffer.PageId)
		newLeafNode.SetPrevPageId(prevLeafPageId)
		leafNode.SetPrevPageId(&newLeafBuffer.PageId)

		// overflowKey は古いリーフノードの先頭のキー (親ノードの境界キーになる)
		overflowKey := leafNode.RecordAt(0).KeyBytes()
		return overflowKey, newLeafPageId, true, nil
	} else if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		// 挿入先の子ノードを取得
		branchNode := node.NewBranchNode(nodeBuffer.GetWriteData())
		childIndex := branchNode.SearchChildSlotNum(record.KeyBytes())
		childPageId := branchNode.ChildPageIdAt(childIndex)
		childNodeBuffer, err := bp.FetchPage(childPageId)
		if err != nil {
			return nil, page.INVALID_PAGE_ID, false, err
		}
		defer bp.UnRefPage(childPageId)

		// 子ノードに対して挿入処理を再帰的に実行
		overflowKeyFromChild, overflowChildPageId, leafSplit, err := bt.insertRecursively(bp, childNodeBuffer, record)
		if err != nil {
			return nil, page.INVALID_PAGE_ID, false, err
		}

		// 子ノードが分割されなかった場合、終了
		childPageSplit := !overflowChildPageId.IsInvalid()
		if !childPageSplit {
			return nil, page.INVALID_PAGE_ID, leafSplit, nil
		}

		// 子ノードが分割された場合、子ノードから返されたキーとページID をレコードとして、ブランチノードに挿入
		overFlowRecord := node.NewRecord(nil, overflowKeyFromChild, overflowChildPageId.ToBytes())
		if branchNode.Insert(childIndex, overFlowRecord) {
			return nil, page.INVALID_PAGE_ID, leafSplit, nil
		}

		// ブランチノードが満杯で挿入に失敗した場合、分割する
		newBranchPageId, err := bp.AllocatePageId(bt.MetaPageId.FileId)
		if err != nil {
			return nil, page.INVALID_PAGE_ID, false, err
		}
		newBranchBuffer, err := bp.AddPage(newBranchPageId)
		if err != nil {
			return nil, page.INVALID_PAGE_ID, false, err
		}
		defer bp.UnRefPage(newBranchPageId)
		newBranchNode := node.NewBranchNode(newBranchBuffer.GetWriteData())
		overflowKey, err := branchNode.SplitInsert(newBranchNode, overFlowRecord)
		if err != nil {
			return nil, page.INVALID_PAGE_ID, false, err
		}

		return overflowKey, newBranchPageId, leafSplit, nil
	}

	panic("unknown node type") // 実際にはここには到達しないので errors.New ではなく panic で良い
}

// ==================================
// Delete
// =================================

// Delete は B+Tree からレコードを削除する
func (bt *BTree) Delete(bp *buffer.BufferPool, key []byte) error {
	// メタページを取得
	metaBuf, err := bp.FetchPage(bt.MetaPageId)
	if err != nil {
		return err
	}
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	meta := newMetaPage(metaBuf.GetReadData())

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
	nodeType := node.GetNodeType(rootPageBuf.GetReadData())
	if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		branchNode := node.NewBranchNode(rootPageBuf.GetReadData())
		if branchNode.NumRecords() == 0 {
			rootCollapsed = true
		}
	}

	// リーフマージもルート縮退も発生しなかった場合、終了
	if !leafMerged && !rootCollapsed {
		return nil
	}

	// リーフマージまたはルート縮退が発生した場合 (この二つは同時に発生する可能性もある)
	meta = newMetaPage(metaBuf.GetWriteData())

	// リーフマージが発生した場合、メタページのリーフページ数を更新
	if leafMerged {
		meta.setLeafPageCount(meta.leafPageCount() - 1)
	}

	// ルートノードの縮退が発生した場合、子ノードをルートにしてメタページを更新
	if rootCollapsed {
		// 子が 1 つになった場合、右端の子をルートにする
		branchNode := node.NewBranchNode(rootPageBuf.GetReadData())
		newRootPageId := branchNode.ChildPageIdAt(0)
		meta.setRootPageId(newRootPageId)
		meta.setHeight(meta.height() - 1)
	}

	return nil
}

// deleteRecursively は再帰的にノードを辿ってレコードを削除する
//
// 戻り値: (アンダーフローが発生したかどうか, リーフマージが発生したかどうか, エラー)
func (bt *BTree) deleteRecursively(bp *buffer.BufferPool, nodeBuffer *buffer.BufferPage, key []byte) (underflow bool, leafMerged bool, err error) {
	nodeType := node.GetNodeType(nodeBuffer.GetReadData())

	if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
		underflow, err = bt.deleteFromLeaf(nodeBuffer, key)
		return underflow, false, err
	} else if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		return bt.deleteFromBranch(bp, nodeBuffer, key)
	}
	panic("unknown node type") // 実際にはここには到達しないので errors.New ではなく panic で良い
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
func (bt *BTree) deleteFromBranch(bp *buffer.BufferPool, nodeBuffer *buffer.BufferPage, key []byte) (underflow bool, leafMerged bool, err error) {
	branchNode := node.NewBranchNode(nodeBuffer.GetWriteData())
	childSlotNum := branchNode.SearchChildSlotNum(key)
	childPageId := branchNode.ChildPageIdAt(childSlotNum)
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
		if childSlotNum < branchNode.NumRecords() {
			siblingPageId := branchNode.ChildPageIdAt(childSlotNum + 1)
			return siblingInfo{pageId: siblingPageId, isLeft: false}
		}
		siblingPageId := branchNode.ChildPageIdAt(childSlotNum - 1)
		return siblingInfo{pageId: siblingPageId, isLeft: true}
	}()

	siblingBuffer, err := bp.FetchPage(sibling.pageId)
	if err != nil {
		return false, false, err
	}
	sibling.bufferPage = siblingBuffer
	defer bp.UnRefPage(sibling.pageId)

	if bytes.Equal(node.GetNodeType(childNodeBuffer.GetReadData()), node.NODE_TYPE_LEAF) {
		uf, lm, err := bt.resolveLeafUnderflow(bp, branchNode, childNodeBuffer, sibling, childSlotNum)
		return uf, leafMerged || lm, err
	}
	uf, err := bt.resolveBranchUnderflow(branchNode, childNodeBuffer, sibling, childSlotNum)
	return uf, leafMerged, err
}

// deleteFromLeaf はリーフノードからレコードを削除する
//
// 戻り値: (アンダーフローが発生したかどうか, エラー)
func (bt *BTree) deleteFromLeaf(nodeBuffer *buffer.BufferPage, key []byte) (underflow bool, err error) {
	// 削除すべきレコード (レコードが格納されているスロット番号) を特定
	leafNode := node.NewLeafNode(nodeBuffer.GetWriteData())
	slotNum, found := leafNode.SearchSlotNum(key)
	if !found {
		return false, ErrKeyNotFound
	}

	// レコードを削除
	leafNode.Delete(slotNum)

	// アンダーフローが発生したかどうかを判定
	return !leafNode.IsHalfFull(), nil
}

// resolveLeafUnderflow はリーフノードのアンダーフロー処理を行う
//
// parentBranchNode: 親のブランチノード
//
// childBuffer: アンダーフローが発生した子ノードのバッファページ (リーフノードのバッファページ)
//
// sibling: childBuffer と兄弟ノードの情報
//
// childSlotNum: childBuffer が親のブランチノードの子ノードの中で何番目か
//
// 戻り値: (アンダーフローが発生したかどうか, リーフマージが発生したかどうか, エラー)
func (bt *BTree) resolveLeafUnderflow(bp *buffer.BufferPool, parentBranchNode *node.BranchNode, childBuffer *buffer.BufferPage, sibling siblingInfo, childSlotNum int) (underflow bool, leafMerged bool, err error) {
	childNode := node.NewLeafNode(childBuffer.GetWriteData())
	siblingNode := node.NewLeafNode(sibling.bufferPage.GetWriteData())

	// 兄弟からレコードを転送できる場合
	if siblingNode.CanTransferRecord(sibling.isLeft) {
		if sibling.isLeft {
			// 左の兄弟から転送: 左の兄弟の末尾レコードを先頭に移動
			lastIndex := siblingNode.NumRecords() - 1
			record := siblingNode.RecordAt(lastIndex)
			childNode.Insert(0, record)
			siblingNode.Delete(lastIndex)
			if !parentBranchNode.Update(childSlotNum-1, childNode.RecordAt(0).KeyBytes()) {
				return false, false, errors.New("failed to update parent branch node key")
			}
		} else {
			// 右の兄弟から転送: 右の兄弟の先頭レコードを末尾に移動
			record := siblingNode.RecordAt(0)
			childNode.Insert(childNode.NumRecords(), record)
			siblingNode.Delete(0)
			if !parentBranchNode.Update(childSlotNum, siblingNode.RecordAt(0).KeyBytes()) {
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
			nextBuf, err := bp.FetchPage(*nextPageId)
			if err != nil {
				return false, false, err
			}
			defer bp.UnRefPage(*nextPageId)
			nextLeaf := node.NewLeafNode(nextBuf.GetWriteData())
			nextLeaf.SetPrevPageId(&sibling.bufferPage.PageId)
		}
		// 親の右端のレコードは不要になるので削除し、RightChild を兄弟ノードに更新
		parentBranchNode.Delete(parentBranchNode.NumRecords() - 1)
		parentBranchNode.SetRightChildPageId(sibling.bufferPage.PageId)
	} else {
		// 右の兄弟とマージ: 兄弟(右)のレコードをすべて子(左)に移動、子が残る
		if !childNode.TransferAllFrom(siblingNode) {
			// ノードの容量を超えてマージ不可の場合はアンダーフローを許容する
			return false, false, nil
		}
		childNode.SetNextPageId(siblingNode.NextPageId())
		// 消滅する siblingNode の次のリーフの prevPageId を child に更新する
		if nextPageId := siblingNode.NextPageId(); nextPageId != nil {
			nextBuf, err := bp.FetchPage(*nextPageId)
			if err != nil {
				return false, false, err
			}
			defer bp.UnRefPage(*nextPageId)
			nextLeaf := node.NewLeafNode(nextBuf.GetWriteData())
			nextLeaf.SetPrevPageId(&childBuffer.PageId)
		}

		if childSlotNum+1 == parentBranchNode.NumRecords() {
			// 兄弟が RightChild(右端) の場合、親の右端のレコードを削除し、RightChild を子ノードに更新
			parentBranchNode.Delete(parentBranchNode.NumRecords() - 1)
			parentBranchNode.SetRightChildPageId(childBuffer.PageId)
		} else {
			// 兄弟が RightChild(右端) でない場合、キーを更新してから削除
			nextKey := parentBranchNode.RecordAt(childSlotNum + 1).KeyBytes()
			if !parentBranchNode.Update(childSlotNum, nextKey) {
				return false, false, errors.New("failed to update parent branch node key")
			}
			parentBranchNode.Delete(childSlotNum + 1) // 右の兄弟を削除するので `childIndex + 1`
		}
	}

	return !parentBranchNode.IsHalfFull(), true, nil
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
func (bt *BTree) resolveBranchUnderflow(parentBranchNode *node.BranchNode, childBuffer *buffer.BufferPage, sibling siblingInfo, childSlotNum int) (underflow bool, err error) {
	childNode := node.NewBranchNode(childBuffer.GetWriteData())
	siblingNode := node.NewBranchNode(sibling.bufferPage.GetWriteData())

	// 兄弟からレコードを転送できる場合
	if siblingNode.CanTransferRecord(sibling.isLeft) {
		if sibling.isLeft {
			// 左の兄弟から転送: 親の境界キーを子の先頭に下ろし、兄弟の末尾キーを親に上げる
			parentRecord := parentBranchNode.RecordAt(childSlotNum - 1)
			siblingRightChild := siblingNode.RightChildPageId()
			childNode.Insert(0, node.NewRecord(nil, parentRecord.KeyBytes(), siblingRightChild.ToBytes()))

			lastIndex := siblingNode.NumRecords() - 1
			siblingLastRecord := siblingNode.RecordAt(lastIndex)
			if !parentBranchNode.Update(childSlotNum-1, siblingLastRecord.KeyBytes()) {
				return false, errors.New("failed to update parent branch node key")
			}
			siblingNode.SetRightChildPageId(page.RestorePageIdFromBytes(siblingLastRecord.NonKeyBytes()))
			siblingNode.Delete(lastIndex)
		} else {
			// 右の兄弟から転送: 親の境界キーを子の末尾に下ろし、兄弟の先頭キーを親に上げる
			parentRecord := parentBranchNode.RecordAt(childSlotNum)
			childRightChild := childNode.RightChildPageId()
			childNode.Insert(childNode.NumRecords(), node.NewRecord(nil, parentRecord.KeyBytes(), childRightChild.ToBytes()))

			siblingFirstRecord := siblingNode.RecordAt(0)
			childNode.SetRightChildPageId(page.RestorePageIdFromBytes(siblingFirstRecord.NonKeyBytes()))
			if !parentBranchNode.Update(childSlotNum, siblingFirstRecord.KeyBytes()) {
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
		parentRecord := parentBranchNode.RecordAt(parentBranchNode.NumRecords() - 1)
		siblingRightChild := siblingNode.RightChildPageId()
		siblingNode.Insert(siblingNode.NumRecords(), node.NewRecord(nil, parentRecord.KeyBytes(), siblingRightChild.ToBytes()))

		siblingNode.TransferAllFrom(childNode)
		siblingNode.SetRightChildPageId(childNode.RightChildPageId())
		// 親の右端のレコードは不要になるので削除し、RightChild を兄弟ノードに更新
		parentBranchNode.Delete(parentBranchNode.NumRecords() - 1)
		parentBranchNode.SetRightChildPageId(sibling.bufferPage.PageId)
	} else {
		// 右の兄弟とマージ: 兄弟(右)のレコードをすべて子(左)に移動、子が残る
		parentRecord := parentBranchNode.RecordAt(childSlotNum)
		childRightChild := childNode.RightChildPageId()
		childNode.Insert(childNode.NumRecords(), node.NewRecord(nil, parentRecord.KeyBytes(), childRightChild.ToBytes()))

		childNode.TransferAllFrom(siblingNode)
		childNode.SetRightChildPageId(siblingNode.RightChildPageId())

		if childSlotNum+1 == parentBranchNode.NumRecords() {
			// 兄弟が RightChild(右端) の場合、親の右端のレコードを削除し、RightChild を子ノードに更新
			parentBranchNode.Delete(parentBranchNode.NumRecords() - 1)
			parentBranchNode.SetRightChildPageId(childBuffer.PageId)
		} else {
			// 兄弟が RightChild(右端) でない場合、キーを更新してから削除
			nextKey := parentBranchNode.RecordAt(childSlotNum + 1).KeyBytes()
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

// Update は B+Tree の特定のノードの値を更新する
//
// record.KeyBytes() で対象のリーフノードを特定し、record.NonKeyBytes() で値を上書きする
func (bt *BTree) Update(bp *buffer.BufferPool, record node.Record) error {
	// メタページを取得
	metaBuf, err := bp.FetchPage(bt.MetaPageId)
	if err != nil {
		return err
	}
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	meta := newMetaPage(metaBuf.GetReadData())

	// ルートページを取得
	rootPageId := meta.rootPageId()
	rootPageBuf, err := bp.FetchPage(rootPageId)
	if err != nil {
		return err
	}

	return bt.updateRecursively(bp, rootPageBuf, record)
}

// updateRecursively は再帰的にノードを辿って該当のリーフノードを見つけ、レコードを更新する
func (bt *BTree) updateRecursively(bp *buffer.BufferPool, nodeBuffer *buffer.BufferPage, record node.Record) error {
	nodeType := node.GetNodeType(nodeBuffer.GetReadData())

	if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		// ブランチノードの場合、再帰呼び出し後に UnRefPage を呼び出す
		defer bp.UnRefPage(nodeBuffer.PageId)

		branchNode := node.NewBranchNode(nodeBuffer.GetReadData())

		// record.KeyBytes() を使って子ノードを特定
		searchMode := SearchModeKey{Key: record.KeyBytes()}
		childPageId := searchMode.childPageId(branchNode)
		childNodeBuffer, err := bp.FetchPage(childPageId)
		if err != nil {
			return err
		}

		// 再帰呼び出し
		return bt.updateRecursively(bp, childNodeBuffer, record)
	} else if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
		leafNode := node.NewLeafNode(nodeBuffer.GetWriteData())

		// 該当のキーを持つレコードを見つける
		slotNum, found := leafNode.SearchSlotNum(record.KeyBytes())
		if !found {
			return ErrKeyNotFound
		}

		// レコードの値を新しい値に更新
		if !leafNode.Update(slotNum, record) {
			return errors.New("failed to update record")
		}
		return nil
	}

	panic("unknown node type") // 実際にはここには到達しないので errors.New ではなく panic で良い
}

// ==================================
// Other
// ==================================

// LeafPageCount はメタページからリーフページ数を取得する
func (bt *BTree) LeafPageCount(bp *buffer.BufferPool) (uint64, error) {
	metaBuf, err := bp.FetchPage(bt.MetaPageId)
	if err != nil {
		return 0, err
	}
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、 UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	meta := newMetaPage(metaBuf.GetReadData())
	return meta.leafPageCount(), nil
}

// Height はメタページから B+Tree の高さを取得する
func (bt *BTree) Height(bp *buffer.BufferPool) (uint64, error) {
	metaBuf, err := bp.FetchPage(bt.MetaPageId)
	if err != nil {
		return 0, err
	}
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、 UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	meta := newMetaPage(metaBuf.GetReadData())
	return meta.height(), nil
}
