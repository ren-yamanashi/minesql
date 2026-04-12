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
	err := bp.AddPage(metaPageId)
	if err != nil {
		return nil, err
	}

	metaData, err := bp.GetWritePageData(metaPageId)
	if err != nil {
		return nil, err
	}
	meta := createMetaPage(page.NewPage(metaData))

	// ルートノード (リーフノード) を作成
	rootNodePageId, err := bp.AllocatePageId(metaPageId.FileId)
	if err != nil {
		return nil, err
	}
	err = bp.AddPage(rootNodePageId)
	if err != nil {
		return nil, err
	}
	rootData, err := bp.GetWritePageData(rootNodePageId)
	if err != nil {
		return nil, err
	}
	rootLeaf := node.NewLeaf(page.NewPage(rootData).Body)
	rootLeaf.Initialize()

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

// ----------------------------------------------
// Search
// ----------------------------------------------

// Search は指定された検索モードで B+Tree を検索し、イテレータを返す
//
// 戻り値: リーフノードのイテレータ
func (bt *BTree) Search(bp *buffer.BufferPool, searchMode SearchMode) (*Iterator, error) {
	// メタページを取得
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、 UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	metaData, err := bp.GetReadPageData(bt.MetaPageId)
	if err != nil {
		return nil, err
	}
	meta := newMetaPage(page.NewPage(metaData))

	// ルートページを取得
	rootPageId := meta.rootPageId()

	return bt.searchRecursively(bp, rootPageId, searchMode)
}

// searchRecursively は再起的にノードを辿って該当のリーフノードを見つける
//
// 戻り値: リーフノードのイテレータ
func (bt *BTree) searchRecursively(bp *buffer.BufferPool, nodePageId page.PageId, searchMode SearchMode) (*Iterator, error) {
	nodeData, err := bp.GetReadPageData(nodePageId)
	if err != nil {
		return nil, err
	}
	nodeType := node.GetNodeType(page.NewPage(nodeData).Body)

	if bytes.Equal(nodeType, node.NodeTypeBranch) {
		// ブランチノードの場合、再起呼び出し後に UnRefPage を呼び出す (優先的に evict されたいため、不要になったらすぐ UnRefPage する)
		defer bp.UnRefPage(nodePageId)

		branch := node.NewBranch(page.NewPage(nodeData).Body)

		// 子ノードのページを取得
		childPageId := searchMode.childPageId(branch)

		// 再帰呼び出し
		return bt.searchRecursively(bp, childPageId, searchMode)
	} else if bytes.Equal(nodeType, node.NodeTypeLeaf) {
		leaf := node.NewLeaf(page.NewPage(nodeData).Body)

		nodeBuffer, err := bp.FetchPage(nodePageId)
		if err != nil {
			return nil, err
		}

		switch sm := searchMode.(type) {
		case SearchModeStart:
			return newIterator(bp, *nodeBuffer, 0), nil
		case SearchModeKey:
			slotNum, _ := leaf.SearchSlotNum(sm.Key)
			iter := newIterator(bp, *nodeBuffer, slotNum)
			// 検索対象のキーが現在のリーフノードの末端のレコードより大きい場合、次のリーフノードに進める
			// 例: リーフノードに (1, ...), (3, ...), (5, ...) のレコードが格納されている場合に、キー 6 を検索したいときなど
			// (この場合 `leaf.SearchSlotNum(sm.Key)` は `leaf.NumRecords()` と等しい値を返す)
			// この場合、次のリーフノードに進めてからイテレータを返す
			if leaf.NumRecords() == slotNum {
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

// FindByKey は指定されたキーで B+Tree を検索し、完全一致するレコードとその物理的な位置を返す
//
// キーが見つからない場合は ErrKeyNotFound を返す
func (bt *BTree) FindByKey(bp *buffer.BufferPool, key []byte) (node.Record, page.SlotPosition, error) {
	iter, err := bt.Search(bp, SearchModeKey{Key: key})
	if err != nil {
		return nil, page.SlotPosition{}, err
	}
	pos := page.SlotPosition{
		PageId:  iter.bufferPage.PageId,
		SlotNum: iter.slotNum,
	}
	record, ok := iter.Get()
	if !ok {
		return nil, page.SlotPosition{}, ErrKeyNotFound
	}
	if !bytes.Equal(record.KeyBytes(), key) {
		return nil, page.SlotPosition{}, ErrKeyNotFound
	}
	return record, pos, nil
}

// ----------------------------------------------
// Insert
// ----------------------------------------------

// Insert は B+Tree にレコードを挿入する
func (bt *BTree) Insert(bp *buffer.BufferPool, record node.Record) error {
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
	metaWriteData, err := bp.GetWritePageData(bt.MetaPageId)
	if err != nil {
		return err
	}
	meta = newMetaPage(page.NewPage(metaWriteData))

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
		err = bp.AddPage(newRootPageId)
		if err != nil {
			return err
		}
		newRootData, err := bp.GetWritePageData(newRootPageId)
		if err != nil {
			return err
		}
		newRootBranch := node.NewBranch(page.NewPage(newRootData).Body)
		err = newRootBranch.Initialize(overflowKey, overflowChildPageId, rootPageId)
		if err != nil {
			return err
		}

		meta.setRootPageId(newRootPageId)
		meta.setHeight(meta.height() + 1)
	}

	return nil
}

// insertRecursively は再帰的にノードを辿ってレコードを挿入する
//
// 戻り値: (オーバーフローキー, 新しいページ ID, リーフ分割が発生したか, エラー)
//
// ノード分割が発生しなかった場合、新しいページ ID には InvalidPageId が返る。
// 分割が発生した場合、オーバーフローキーは親ノードに伝播させる境界キーになり、新しいページ ID は分割で作られたノードのページ ID になる。
//
// ※分割挿入発生時にできる新しいノードは、分割元の前に位置するノードとして作られる
func (bt *BTree) insertRecursively(bp *buffer.BufferPool, nodeBuffer *buffer.BufferPage, record node.Record) ([]byte, page.PageId, bool, error) {
	nodeData, err := bp.GetReadPageData(nodeBuffer.PageId)
	if err != nil {
		return nil, page.InvalidPageId, false, err
	}
	nodeType := node.GetNodeType(page.NewPage(nodeData).Body)

	if bytes.Equal(nodeType, node.NodeTypeLeaf) {
		nodeWriteData, err := bp.GetWritePageData(nodeBuffer.PageId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		leaf := node.NewLeaf(page.NewPage(nodeWriteData).Body)
		slotNum, found := leaf.SearchSlotNum(record.KeyBytes())
		if found {
			return nil, page.InvalidPageId, false, ErrDuplicateKey
		}

		// リーフノードに挿入できた場合、終了
		if leaf.Insert(slotNum, record) {
			return nil, page.InvalidPageId, false, nil
		}

		// リーフノードが満杯の場合、分割する
		prevLeafPageId := leaf.PrevPageId()

		// 前のリーフノードが存在する場合、そのページを取得
		if prevLeafPageId != nil {
			defer bp.UnRefPage(*prevLeafPageId)
		}

		// 新しいリーフノードを作成
		newLeafPageId, err := bp.AllocatePageId(bt.MetaPageId.FileId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		err = bp.AddPage(newLeafPageId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		defer bp.UnRefPage(newLeafPageId)

		// 前のリーフノードが存在する場合、そのリーフノードに格納されている nextPageId を、新しいリーフノードのページID に更新する
		if prevLeafPageId != nil {
			prevLeafData, err := bp.GetWritePageData(*prevLeafPageId)
			if err != nil {
				return nil, page.InvalidPageId, false, err
			}
			prevLeaf := node.NewLeaf(page.NewPage(prevLeafData).Body)
			prevLeaf.SetNextPageId(&newLeafPageId)
		}

		// 新しいリーフノードに分割挿入する
		newLeafData, err := bp.GetWritePageData(newLeafPageId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		newLeaf := node.NewLeaf(page.NewPage(newLeafData).Body)
		_, err = leaf.SplitInsert(newLeaf, record)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		newLeaf.SetNextPageId(&nodeBuffer.PageId)
		newLeaf.SetPrevPageId(prevLeafPageId)
		leaf.SetPrevPageId(&newLeafPageId)

		// overflowKey は古いリーフノードの先頭のキー (親ノードの境界キーになる)
		overflowKey := leaf.RecordAt(0).KeyBytes()
		return overflowKey, newLeafPageId, true, nil
	} else if bytes.Equal(nodeType, node.NodeTypeBranch) {
		// 挿入先の子ノードを取得
		nodeWriteData, err := bp.GetWritePageData(nodeBuffer.PageId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		branch := node.NewBranch(page.NewPage(nodeWriteData).Body)
		childIndex := branch.SearchChildSlotNum(record.KeyBytes())
		childPageId := branch.ChildPageIdAt(childIndex)
		childNodeBuffer, err := bp.FetchPage(childPageId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		defer bp.UnRefPage(childPageId)

		// 子ノードに対して挿入処理を再帰的に実行
		overflowKeyFromChild, overflowChildPageId, leafSplit, err := bt.insertRecursively(bp, childNodeBuffer, record)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}

		// 子ノードが分割されなかった場合、終了
		childPageSplit := !overflowChildPageId.IsInvalid()
		if !childPageSplit {
			return nil, page.InvalidPageId, leafSplit, nil
		}

		// 子ノードが分割された場合、子ノードから返されたキーとページID をレコードとして、ブランチノードに挿入
		overFlowRecord := node.NewRecord(nil, overflowKeyFromChild, overflowChildPageId.ToBytes())
		if branch.Insert(childIndex, overFlowRecord) {
			return nil, page.InvalidPageId, leafSplit, nil
		}

		// ブランチノードが満杯で挿入に失敗した場合、分割する
		newBranchPageId, err := bp.AllocatePageId(bt.MetaPageId.FileId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		err = bp.AddPage(newBranchPageId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		defer bp.UnRefPage(newBranchPageId)
		newBranchData, err := bp.GetWritePageData(newBranchPageId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		newBranch := node.NewBranch(page.NewPage(newBranchData).Body)
		overflowKey, err := branch.SplitInsert(newBranch, overFlowRecord)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}

		return overflowKey, newBranchPageId, leafSplit, nil
	}

	panic("unknown node type") // 実際にはここには到達しないので errors.New ではなく panic で良い
}

// ----------------------------------------------
// Delete
// ----------------------------------------------

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
func (bt *BTree) deleteRecursively(bp *buffer.BufferPool, nodeBuffer *buffer.BufferPage, key []byte) (underflow bool, leafMerged bool, err error) {
	nodeData, err := bp.GetReadPageData(nodeBuffer.PageId)
	if err != nil {
		return false, false, err
	}
	nodeType := node.GetNodeType(page.NewPage(nodeData).Body)

	if bytes.Equal(nodeType, node.NodeTypeLeaf) {
		underflow, err = bt.deleteFromLeaf(bp, nodeBuffer, key)
		return underflow, false, err
	} else if bytes.Equal(nodeType, node.NodeTypeBranch) {
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
func (bt *BTree) resolveLeafUnderflow(bp *buffer.BufferPool, parentBranch *node.Branch, childBuffer *buffer.BufferPage, sibling siblingInfo, childSlotNum int) (underflow bool, leafMerged bool, err error) {
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
func (bt *BTree) resolveBranchUnderflow(bp *buffer.BufferPool, parentBranch *node.Branch, childBuffer *buffer.BufferPage, sibling siblingInfo, childSlotNum int) (underflow bool, err error) {
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

// ----------------------------------------------
// Update
// ----------------------------------------------

// Update は B+Tree の特定のノードの値を更新する
//
// record.KeyBytes() で対象のリーフノードを特定し、record.NonKeyBytes() で値を上書きする
func (bt *BTree) Update(bp *buffer.BufferPool, record node.Record) error {
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

	return bt.updateRecursively(bp, rootPageBuf, record)
}

// updateRecursively は再帰的にノードを辿って該当のリーフノードを見つけ、レコードを更新する
func (bt *BTree) updateRecursively(bp *buffer.BufferPool, nodeBuffer *buffer.BufferPage, record node.Record) error {
	nodeData, err := bp.GetReadPageData(nodeBuffer.PageId)
	if err != nil {
		return err
	}
	nodeType := node.GetNodeType(page.NewPage(nodeData).Body)

	if bytes.Equal(nodeType, node.NodeTypeBranch) {
		// ブランチノードの場合、再帰呼び出し後に UnRefPage を呼び出す
		defer bp.UnRefPage(nodeBuffer.PageId)

		branch := node.NewBranch(page.NewPage(nodeData).Body)

		// record.KeyBytes() を使って子ノードを特定
		searchMode := SearchModeKey{Key: record.KeyBytes()}
		childPageId := searchMode.childPageId(branch)
		childNodeBuffer, err := bp.FetchPage(childPageId)
		if err != nil {
			return err
		}

		// 再帰呼び出し
		return bt.updateRecursively(bp, childNodeBuffer, record)
	} else if bytes.Equal(nodeType, node.NodeTypeLeaf) {
		nodeWriteData, err := bp.GetWritePageData(nodeBuffer.PageId)
		if err != nil {
			return err
		}
		leaf := node.NewLeaf(page.NewPage(nodeWriteData).Body)

		// 該当のキーを持つレコードを見つける
		slotNum, found := leaf.SearchSlotNum(record.KeyBytes())
		if !found {
			return ErrKeyNotFound
		}

		// レコードの値を新しい値に更新
		if !leaf.Update(slotNum, record) {
			return errors.New("failed to update record")
		}
		return nil
	}

	panic("unknown node type") // 実際にはここには到達しないので errors.New ではなく panic で良い
}

// ----------------------------------------------
// Other
// ----------------------------------------------

// LeafPageCount はメタページからリーフページ数を取得する
func (bt *BTree) LeafPageCount(bp *buffer.BufferPool) (uint64, error) {
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、 UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	metaData, err := bp.GetReadPageData(bt.MetaPageId)
	if err != nil {
		return 0, err
	}
	meta := newMetaPage(page.NewPage(metaData))
	return meta.leafPageCount(), nil
}

// Height はメタページから B+Tree の高さを取得する
func (bt *BTree) Height(bp *buffer.BufferPool) (uint64, error) {
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、 UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	metaData, err := bp.GetReadPageData(bt.MetaPageId)
	if err != nil {
		return 0, err
	}
	meta := newMetaPage(page.NewPage(metaData))
	return meta.height(), nil
}
