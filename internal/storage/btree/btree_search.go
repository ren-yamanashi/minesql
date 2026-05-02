package btree

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// Search は指定された検索モードで B+Tree を検索する
func (bt *Btree) Search(mode SearchMode) (*Iterator, error) {
	// メタページ取得
	pageMeta, err := bt.bufferPool.GetReadPage(bt.MetaPageId)
	if err != nil {
		return nil, err
	}
	defer bt.bufferPool.UnRefPage(bt.MetaPageId)
	metaPage := newMetaPage(pageMeta)

	// ルートページ取得
	rootPageId := metaPage.rootPageId()

	return bt.searchRecursively(rootPageId, mode)
}

// searchRecursively は再帰的にノードを辿って該当のリーフノードを見つける
func (bt *Btree) searchRecursively(nodePageId page.PageId, mode SearchMode) (*Iterator, error) {
	bufPage, err := bt.bufferPool.FetchPage(nodePageId)
	if err != nil {
		return nil, err
	}
	nodeType := node.GetNodeType(bufPage.Page)

	switch {
	// ブランチノードの場合、子ノードに対して再帰探索する
	case bytes.Equal(nodeType, node.NodeTypeBranch):
		defer bt.bufferPool.UnRefPage(nodePageId)
		branchNode := node.NewBranchNode(bufPage.Page)
		childPageId, err := mode.childPageId(branchNode)
		if err != nil {
			return nil, err
		}
		return bt.searchRecursively(childPageId, mode)

	// リーフノードの場合、検索モードに応じて探索する
	case bytes.Equal(nodeType, node.NodeTypeLeaf):
		leafNode := node.NewLeafNode(bufPage.Page)

		switch sm := mode.(type) {
		case SearchModeStart:
			return NewIterator(bt.bufferPool, *bufPage, 0), nil
		case SearchModeKey:
			slotNum, _ := leafNode.SearchSlotNum(sm.Key)
			iter := NewIterator(bt.bufferPool, *bufPage, slotNum)
			// 検索対象のキーが現在のリーフノードの末端のレコードより大きい場合、次のリーフノードに進める
			// 例: リーフノードに (1, ...), (3, ...), (5, ...) のレコードが格納されている場合に、キー 6 を検索したいときなど
			// (この場合 `leaf.SearchSlotNum(sm.Key)` は `leaf.NumRecords()` と等しい値を返す)
			// この場合、次のリーフノードに進めてからイテレータを返す
			if leafNode.NumRecords() == slotNum {
				err := iter.Advance()
				if err != nil {
					return nil, err
				}
			}
			return iter, nil
		default:
			return nil, errors.New("unknown search mode")
		}

	default:
		return nil, errors.New("unknown node type")
	}
}

// FindByKey は指定されたキーで B+Tree を検索し、完全一致するレコードとその物理的な位置を返す (キーが見つからない場合は ErrKeyNotFound)
func (bt *Btree) FindByKey(key []byte) (node.Record, node.RecordPosition, error) {
	iter, err := bt.Search(SearchModeKey{Key: key})
	if err != nil {
		return nil, node.RecordPosition{}, err
	}
	position := node.RecordPosition{
		PageId:  iter.BufferPage.PageId,
		SlotNum: iter.SlotNum,
	}
	record, ok, err := iter.Get()
	if err != nil {
		return nil, node.RecordPosition{}, err
	}
	if !ok {
		return nil, node.RecordPosition{}, ErrKeyNotFound
	}
	if !bytes.Equal(record.Key(), key) {
		return nil, node.RecordPosition{}, ErrKeyNotFound
	}
	return record, position, nil
}

// LeafPageIds はブランチページのみ辿り、全リーフページの PageId を収集する
func (bt *Btree) LeafPageIds() ([]page.PageId, error) {
	pageMeta, err := bt.bufferPool.GetReadPage(bt.MetaPageId)
	if err != nil {
		return nil, err
	}
	defer bt.bufferPool.UnRefPage(bt.MetaPageId)
	metaPage := newMetaPage(pageMeta)
	rootPageId := metaPage.rootPageId()
	height := metaPage.height()

	// 高さ 1: ルートがリーフ
	if height <= 1 {
		return []page.PageId{rootPageId}, nil
	}

	// 高さ 2 以上: ブランチノードを辿ってリーフの PageId を収集
	// 幅優先でブランチレベルを 1 つずつ降りていく
	currentLevel := []page.PageId{rootPageId}
	for range height - 1 {
		var nextLevel []page.PageId
		for _, nodePageId := range currentLevel {
			pg, err := bt.bufferPool.GetReadPage(nodePageId)
			if err != nil {
				return nil, err
			}
			bt.bufferPool.UnRefPage(nodePageId)
			branchNode := node.NewBranchNode(pg)

			for idx := range branchNode.NumRecords() {
				childPageId, err := branchNode.ChildPageId(idx)
				if err != nil {
					return nil, err
				}
				nextLevel = append(nextLevel, childPageId)
			}
			nextLevel = append(nextLevel, branchNode.RightChildPageId())
		}
		currentLevel = nextLevel
	}
	return currentLevel, nil
}
