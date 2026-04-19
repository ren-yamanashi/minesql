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

	switch {
	case bytes.Equal(nodeType, node.NodeTypeBranch):
		// ブランチノードの場合、再起呼び出し後に UnRefPage を呼び出す (優先的に evict されたいため、不要になったらすぐ UnRefPage する)
		defer bp.UnRefPage(nodePageId)

		branch := node.NewBranch(page.NewPage(nodeData).Body)

		// 子ノードのページを取得
		childPageId := searchMode.childPageId(branch)

		// 再帰呼び出し
		return bt.searchRecursively(bp, childPageId, searchMode)

	case bytes.Equal(nodeType, node.NodeTypeLeaf):
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
		default:
			panic("unknown search mode")
		}

	default:
		panic("unknown node type")
	}
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

// LeafPageIds はブランチページのみを辿り、全リーフページの PageId を収集する
//
// リーフページ自体は読まないため、バッファプールのキャッシュ状態に影響しない
// (page_read_cost の in_mem 算出で使用する)
func (bt *BTree) LeafPageIds(bp *buffer.BufferPool) ([]page.PageId, error) {
	defer bp.UnRefPage(bt.MetaPageId)
	metaData, err := bp.GetReadPageData(bt.MetaPageId)
	if err != nil {
		return nil, err
	}
	meta := newMetaPage(page.NewPage(metaData))
	rootPageId := meta.rootPageId()
	height := meta.height()

	// 高さ 1: ルートがリーフ
	if height <= 1 {
		return []page.PageId{rootPageId}, nil
	}

	// 高さ 2 以上: ブランチを辿ってリーフの PageId を収集
	// 幅優先でブランチレベルを 1 つずつ降りていく
	currentLevel := []page.PageId{rootPageId}
	for level := uint64(1); level < height; level++ {
		var nextLevel []page.PageId
		for _, nodePageId := range currentLevel {
			data, err := bp.GetReadPageData(nodePageId)
			if err != nil {
				return nil, err
			}
			bp.UnRefPage(nodePageId)
			branch := node.NewBranch(page.NewPage(data).Body)
			for i := 0; i <= branch.NumRecords(); i++ {
				nextLevel = append(nextLevel, branch.ChildPageIdAt(i))
			}
		}
		currentLevel = nextLevel
	}

	return currentLevel, nil
}

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
