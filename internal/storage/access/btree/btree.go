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
func CreateBTree(bpm *bufferpool.BufferPoolManager) (*BTree, error) {
	// メタページを作成
	metaPageId := bpm.DiskManager.AllocatePage()
	metaBuf, err := bpm.AddPage(metaPageId)
	if err != nil {
		return nil, err
	}
	defer bpm.UnRefPage(metaPageId)
	meta := metapage.NewMetaPage(metaBuf.Page[:])

	// ルートノード (リーフノード) を作成
	rootNodePageId := bpm.DiskManager.AllocatePage()
	rootBuf, err := bpm.AddPage(rootNodePageId)
	if err != nil {
		return nil, err
	}
	defer bpm.UnRefPage(rootNodePageId)
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

// ルートページを取得
func (bt *BTree) fetchRootPage(bpm *bufferpool.BufferPoolManager) (*bufferpool.BufferPage, error) {
	// メタページを取得
	metaBuf, err := bpm.FetchPage(bt.MetaPageId)
	if err != nil {
		return nil, err
	}
	defer bpm.UnRefPage(bt.MetaPageId)
	meta := metapage.NewMetaPage(metaBuf.Page[:])

	// ルートページを取得
	rootPageId := meta.RootPageId()
	return bpm.FetchPage(rootPageId)
}

func (bt *BTree) Search(bpm *bufferpool.BufferPoolManager, searchMode SearchMode) (*Iterator, error) {
	rootPage, err := bt.fetchRootPage(bpm)
	if err != nil {
		return nil, err
	}
	return bt.searchInternal(bpm, rootPage, searchMode)
}

func (bt *BTree) Insert(bpm *bufferpool.BufferPoolManager, pair node.Pair) error {
	metaPageBuf, err := bpm.FetchPage(bt.MetaPageId)
	if err != nil {
		return err
	}
	defer bpm.UnRefPage(bt.MetaPageId)
	meta := metapage.NewMetaPage(metaPageBuf.Page[:])
	rootPageId := meta.RootPageId()
	rootPageBuf, err := bpm.FetchPage(rootPageId)
	if err != nil {
		return err
	}
	defer bpm.UnRefPage(rootPageId)

	overflowKey, overflowChildPageId, err := bt.insertInternal(bpm, rootPageBuf, pair)
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
	metaPageBuf.IsDirty = true
	return nil
}

// 検索 (再起呼び出し用の内部関数)
func (bt *BTree) searchInternal(bpm *bufferpool.BufferPoolManager, nodeBuffer *bufferpool.BufferPage, searchMode SearchMode) (*Iterator, error) {
	_node := node.NewNode(nodeBuffer.Page[:])
	nodeType := _node.NodeType()

	if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
		// リーフノードの場合、nodeBuffer はイテレータに格納される (そのため UnRefPage は呼ばれない)
		leafNode := node.NewLeafNode(_node.Body())
		var bufferId int

		switch sm := searchMode.(type) {
		case SearchModeStart:
			bufferId = 0
		case SearchModeKey:
			var found bool
			bufferId, found = leafNode.SearchBufferId(sm.Key)
			if !found {
				// 見つからなかった場合は挿入位置を返す
			}
		}

		isRightMost := leafNode.NumPairs() == bufferId
		iter := NewIterator(*nodeBuffer, bufferId)

		if isRightMost {
			iter.Advance(bpm)
		}

		return iter, nil
	} else if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		// ブランチノードの場合、再起呼び出し後に UnRefPage を呼び出す
		defer bpm.UnRefPage(nodeBuffer.PageId)

		branchNode := node.NewBranchNode(_node.Body())
		var childPageId disk.PageId

		switch sm := searchMode.(type) {
		case SearchModeStart:
			childPageId = sm.ChildPageId(branchNode)
		case SearchModeKey:
			childPageId = sm.ChildPageId(branchNode)
		}

		childNodePage, err := bpm.FetchPage(childPageId)
		if err != nil {
			return nil, err
		}

		// 再帰呼び出し
		return bt.searchInternal(bpm, childNodePage, searchMode)
	}

	panic("unknown node type")
}

// 戻り値: (オーバーフローキー, 新しいページ ID, エラー)
func (bt *BTree) insertInternal(bpm *bufferpool.BufferPoolManager, nodeBuffer *bufferpool.BufferPage, pair node.Pair) ([]byte, *disk.PageId, error) {
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
		leafNode.SetNextPageId(&newLeafBuffer.PageId)

		newNode := node.NewNode(newLeafBuffer.Page[:])
		newNode.InitAsLeafNode()
		newLeafNode := node.NewLeafNode(newNode.Body())
		newLeafNode.Initialize()
		overflowKey := leafNode.SplitInsert(newLeafNode, pair)
		newLeafNode.SetNextPageId(&nodeBuffer.PageId)
		newLeafNode.SetPrevPageId(prevLeafPageId)
		nodeBuffer.IsDirty = true

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

		overflowKeyFromChild, overflowChildPageId, err := bt.insertInternal(bpm, childNodeBuffer, pair)
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
