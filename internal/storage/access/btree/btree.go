package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	metapage "minesql/internal/storage/access/btree/meta_page"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

var (
	ErrDuplicateKey = errors.New("duplicate key")
)

type BTree struct {
	MetaPageId disk.PageId
}

// 新しい B+Tree を作成
// メタページとルートノード (リーフノード) を作成する (メタページにはルートノードのページIDを設定する)
func CreateBTree(bpm *bufferpool.BufferPoolManager) (*BTree, error) {
	// メタページを作成
	metaPageId := bpm.DiskManager.AllocatePage()
	metaBuf, err := bpm.AddPage(metaPageId)
	if err != nil {
		return nil, err
	}
	meta := metapage.NewMetaPage(metaBuf.Page[:])

	// ルートノード (リーフノード) を作成
	rootNodePageId := bpm.DiskManager.AllocatePage()
	rootBuf, err := bpm.AddPage(rootNodePageId)
	if err != nil {
		return nil, err
	}
	rootNode := node.NewNode(rootBuf.Page[:])
	rootNode.InitAsLeafNode()
	rootLeafNode := node.NewLeafNode(rootNode.Body())
	rootLeafNode.Initialize()

	// メタページにルートページIDを設定
	meta.SetRootPageId(rootNodePageId)

	return &BTree{MetaPageId: metaPageId}, nil
}

// 既存の B+Tree を開く
func NewBTree(metaPageId disk.PageId) *BTree {
	return &BTree{MetaPageId: metaPageId}
}

func (bt *BTree) Search(bpm *bufferpool.BufferPoolManager, searchMode SearchMode) (*Iterator, error) {
	rootPage, err := bt.fetchRootPage(bpm)
	if err != nil {
		return nil, err
	}
	return bt.searchRecursively(bpm, rootPage, searchMode)
}

func (bt *BTree) Insert(bpm *bufferpool.BufferPoolManager, pair node.Pair) error {
	metaBuf, err := bpm.FetchPage(bt.MetaPageId)
	if err != nil {
		return err
	}
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、 UnRefPage する
	defer bpm.UnRefPage(bt.MetaPageId)
	meta := metapage.NewMetaPage(metaBuf.Page[:])
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
	newRootPageId := bpm.DiskManager.AllocatePage()
	newRootBuf, err := bpm.AddPage(newRootPageId)
	if err != nil {
		return err
	}
	defer bpm.UnRefPage(newRootPageId)
	newRootNode := node.NewNode(newRootBuf.Page[:])
	newRootNode.InitAsBranchNode()
	newRootBranchNode := node.NewBranchNode(newRootNode.Body())
	newRootBranchNode.Initialize(overflowKey, *overflowChildPageId, rootPageId)
	meta.SetRootPageId(newRootBuf.PageId)
	metaBuf.IsDirty = true
	newRootBuf.IsDirty = true
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
	meta := metapage.NewMetaPage(metaBuf.Page[:])

	// ルートページを取得
	rootPageId := meta.RootPageId()
	return bpm.FetchPage(rootPageId)
}

// 再起的にノードを辿って該当のリーフノードを見つける
// 戻り値: リーフノードのイテレータ
func (bt *BTree) searchRecursively(bpm *bufferpool.BufferPoolManager, nodeBuffer *bufferpool.BufferPage, searchMode SearchMode) (*Iterator, error) {
	_node := node.NewNode(nodeBuffer.Page[:])
	nodeType := _node.NodeType()

	if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		// ブランチノードの場合、再起呼び出し後に UnRefPage を呼び出す (優先的に evict されたいため、不要になったらすぐ UnRefPage する)
		defer bpm.UnRefPage(nodeBuffer.PageId)

		branchNode := node.NewBranchNode(_node.Body())

		// 子ノードのページを取得
		childPageId := (func() disk.PageId {
			switch sm := searchMode.(type) {
			case SearchModeStart:
				return sm.ChildPageId(branchNode)
			case SearchModeKey:
				return sm.ChildPageId(branchNode)
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
		leafNode := node.NewLeafNode(_node.Body())
		bufferId := (func() int {
			switch sm := searchMode.(type) {
			case SearchModeStart:
				return 0
			case SearchModeKey:
				bId, _ := leafNode.SearchBufferId(sm.Key)
				return bId
			}
			panic("unreachable")
		})()

		iter := NewIterator(*nodeBuffer, bufferId)

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
func (bt *BTree) insertRecursively(bpm *bufferpool.BufferPoolManager, nodeBuffer *bufferpool.BufferPage, pair node.Pair) ([]byte, *disk.PageId, error) {
	_node := node.NewNode(nodeBuffer.Page[:])
	nodeType := _node.NodeType()

	if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
		leafNode := node.NewLeafNode(_node.Body())
		bufferId, found := leafNode.SearchBufferId(pair.Key)
		if found {
			return nil, nil, ErrDuplicateKey
		}

		if leafNode.Insert(bufferId, pair) {
			nodeBuffer.IsDirty = true
			return nil, nil, nil
		}

		// リーフノードが満杯の場合、分割する
		prevLeafPageId := leafNode.PrevPageId()
		var prevLeafBuffer *bufferpool.BufferPage
		var err error
		if prevLeafPageId != nil {
			prevLeafBuffer, err = bpm.FetchPage(*prevLeafPageId)
			if err != nil {
				return nil, nil, err
			}
			defer bpm.UnRefPage(*prevLeafPageId)
		}

		newLeafPageId := bpm.DiskManager.AllocatePage()
		newLeafBuffer, err := bpm.AddPage(newLeafPageId)
		if err != nil {
			return nil, nil, err
		}
		defer bpm.UnRefPage(newLeafPageId)

		if prevLeafBuffer != nil {
			prevNode := node.NewNode(prevLeafBuffer.Page[:])
			prevLeafNode := node.NewLeafNode(prevNode.Body())
			prevLeafNode.SetNextPageId(&newLeafBuffer.PageId)
			prevLeafBuffer.IsDirty = true
		}
	
		newNode := node.NewNode(newLeafBuffer.Page[:])
		newNode.InitAsLeafNode()
		newLeafNode := node.NewLeafNode(newNode.Body())
		newLeafNode.Initialize()
		leafNode.SplitInsert(newLeafNode, pair)
		newLeafNode.SetNextPageId(&nodeBuffer.PageId)
		newLeafNode.SetPrevPageId(prevLeafPageId)
		leafNode.SetPrevPageId(&newLeafBuffer.PageId)
		nodeBuffer.IsDirty = true
		newLeafBuffer.IsDirty = true

		// overflowKey は古いリーフノードの最初のキー (親ノードの境界キーになる)
		overflowKey := leafNode.PairAt(0).Key
		return overflowKey, &newLeafBuffer.PageId, nil
	} else if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		branchNode := node.NewBranchNode(_node.Body())
		childIndex := branchNode.SearchChildIdx(pair.Key)
		childPageId := branchNode.ChildPageIdAt(childIndex)
		childNodeBuffer, err := bpm.FetchPage(childPageId)
		if err != nil {
			return nil, nil, err
		}
		defer bpm.UnRefPage(childPageId)

		overflowKeyFromChild, overflowChildPageId, err := bt.insertRecursively(bpm, childNodeBuffer, pair)
		if err != nil {
			return nil, nil, err
		}

		if overflowChildPageId == nil {
			return nil, nil, nil
		}

		overFlowPair := node.NewPair(overflowKeyFromChild, pageIdToBytes(*overflowChildPageId))
		if branchNode.Insert(childIndex, overFlowPair) {
			nodeBuffer.IsDirty = true
			return nil, nil, nil
		}

		// ブランチノードが満杯の場合、分割する
		nodeBranchBuffer := bpm.DiskManager.AllocatePage()
		newBranchBuffer, err := bpm.AddPage(nodeBranchBuffer)
		if err != nil {
			return nil, nil, err
		}
		defer bpm.UnRefPage(nodeBranchBuffer)
		newNode := node.NewNode(newBranchBuffer.Page[:])
		newNode.InitAsBranchNode()
		newBranchNode := node.NewBranchNode(newNode.Body())
		overflowKey := branchNode.SplitInsert(newBranchNode, overFlowPair)
		nodeBuffer.IsDirty = true
		newBranchBuffer.IsDirty = true

		return overflowKey, &newBranchBuffer.PageId, nil
	}

	panic("unknown node type")
}

func pageIdToBytes(id disk.PageId) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(id))
	return b
}
