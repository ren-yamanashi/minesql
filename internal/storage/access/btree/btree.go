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
func (bt *BTree) Search(bpm *bufferpool.BufferPoolManager, searchMode SearchMode) (*Iterator, error) {
	rootPage, err := bt.fetchRootPage(bpm)
	if err != nil {
		return nil, err
	}
	return bt.searchRecursively(bpm, rootPage, searchMode)
}

// B+Tree にペアを挿入する
func (bt *BTree) Insert(bpm *bufferpool.BufferPoolManager, pair node.Pair) error {
	metaBuf, err := bpm.FetchPage(bt.MetaPageId)
	if err != nil {
		return err
	}
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、 UnRefPage する
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
			panic("unreachable")
		})()
		childNodePage, err := bpm.FetchPage(childPageId)
		if err != nil {
			return nil, err
		}

		// 再帰呼び出し
		return bt.searchRecursively(bpm, childNodePage, searchMode)
	} else if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
		leafNode := node.NewLeafNode(nodeBuffer.GetReadData())
		bufferId := (func() int {
			switch sm := searchMode.(type) {
			case SearchModeStart:
				return 0
			case SearchModeKey:
				bId, _ := leafNode.SearchSlotNum(sm.Key)
				return bId
			}
			panic("unreachable")
		})()

		iter := newIterator(*nodeBuffer, bufferId)

		// 検索対象のキーが現在のリーフノードの末端のペアより大きい場合、次のリーフノードに進める
		// 例えば、リーフノードに (1, ...), (3, ...), (5, ...) のペアが格納されている場合に、キー 6 を検索したいときなど
		// この場合、次のリーフノードに進めてからイテレータを返す
		if leafNode.NumPairs() == bufferId {
			iter.Advance(bpm)
		}

		return iter, nil
	}

	panic("unknown node type")
}

// 戻り値: (オーバーフローキー, 新しいページ ID, エラー)
func (bt *BTree) insertRecursively(bpm *bufferpool.BufferPoolManager, nodeBuffer *bufferpool.BufferPage, pair node.Pair) ([]byte, *page.PageId, error) {
	nodeType := node.GetNodeType(nodeBuffer.GetReadData())

	if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
		leafNode := node.NewLeafNode(nodeBuffer.GetWriteData())
		bufferId, found := leafNode.SearchSlotNum(pair.Key)
		if found {
			return nil, nil, ErrDuplicateKey
		}

		// リーフノードに挿入できた場合、終了
		if leafNode.Insert(bufferId, pair) {
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

	panic("unknown node type")
}
