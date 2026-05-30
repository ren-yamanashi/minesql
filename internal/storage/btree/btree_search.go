package btree

import (
	"bytes"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// Search は指定された検索モードで B+Tree を検索する
func (t *Tree) Search(mtr *buffer.Mtr, mode SearchMode) (*Iterator, error) {
	// メタページ取得
	pageMeta, err := mtr.PageForRead(t.MetaPageId())
	if err != nil {
		return nil, err
	}
	defer mtr.Unpin(t.MetaPageId())
	metaPage := newMetaPage(pageMeta.Data())

	// ルートページ取得
	rootPageId := metaPage.rootPageId()

	return t.searchRecursively(mtr, rootPageId, mode)
}

// FindByKey は指定されたキーで B+Tree を検索し、完全一致するレコードとその物理的な位置を返す (キーが見つからない場合は ErrKeyNotFound)
func (t *Tree) FindByKey(mtr *buffer.Mtr, key []byte) (Record, RecordPosition, error) {
	iter, err := t.Search(mtr, SearchModeKey{Key: key})
	if err != nil {
		return nil, RecordPosition{}, err
	}
	defer iter.Close()
	position := RecordPosition{
		PageId:  iter.bufferPage.PageId(),
		SlotNum: iter.slotNum,
	}
	record, ok, err := iter.Get()
	if err != nil {
		return nil, RecordPosition{}, err
	}
	if !ok {
		return nil, RecordPosition{}, ErrKeyNotFound
	}
	if !bytes.Equal(record.Key(), key) {
		return nil, RecordPosition{}, ErrKeyNotFound
	}
	return record, position, nil
}

// searchRecursively は再帰的にノードを辿って該当のリーフノードを見つける
func (t *Tree) searchRecursively(mtr *buffer.Mtr, nodePageId page.Id, mode SearchMode) (*Iterator, error) {
	bufPage, err := mtr.PageForRead(nodePageId)
	if err != nil {
		return nil, err
	}
	nt := nodeType(bufPage.Data())

	switch nt {
	// ブランチノードの場合、子ノードに対して再帰探索する
	case nodeTypeBranch:
		defer mtr.Unpin(nodePageId)
		branchNode := newBranchNode(bufPage.Data())
		childPageId, err := mode.childPageId(branchNode)
		if err != nil {
			return nil, err
		}
		return t.searchRecursively(mtr, childPageId, mode)

	// リーフノードの場合、検索モードに応じて探索する
	case nodeTypeLeaf:
		leafNode := newLeafNode(bufPage.Data())
		slotNum := mode.slotNum(leafNode)
		iter := NewIterator(t.bufferPool, *bufPage, slotNum)
		// リーフの Pin は走査側 (Iterator) が引き継ぐため、mtr の管理から外す
		mtr.Detach(nodePageId)
		// 検索対象のキーが現在のリーフノードの末端のレコードより大きい場合、次のリーフノードに進める
		// 例: リーフノードに (1, ...), (3, ...), (5, ...) のレコードが格納されている場合に、キー 6 を検索したいときなど
		// (この場合 SearchSlotNum は NumRecords と等しい値を返す)
		// この場合、次のリーフノードに進めてからイテレータを返す
		if leafNode.numRecords() == slotNum {
			err := iter.Advance()
			if err != nil {
				iter.Close()
				return nil, err
			}
		}
		return iter, nil

	default:
		mtr.Unpin(nodePageId)
		return nil, errUnknownNodeType
	}
}

// leafPageIds はブランチページのみ辿り、全リーフページの PageId を収集する
func (t *Tree) leafPageIds() ([]page.Id, error) {
	mtr := buffer.NewMtr(t.bufferPool)
	defer mtr.UnpinAll()
	pageMeta, err := mtr.PageForRead(t.MetaPageId())
	if err != nil {
		return nil, err
	}
	metaPage := newMetaPage(pageMeta.Data())
	rootPageId := metaPage.rootPageId()
	height := metaPage.height()

	// 高さ 1: ルートがリーフ
	if height <= 1 {
		return []page.Id{rootPageId}, nil
	}

	// 高さ 2 以上: ブランチノードを辿ってリーフの PageId を収集
	// 幅優先でブランチレベルを 1 つずつ降りていく
	currentLevel := []page.Id{rootPageId}
	for range height - 1 {
		var nextLevel []page.Id
		for _, nodePageId := range currentLevel {
			pg, err := mtr.PageForRead(nodePageId)
			if err != nil {
				return nil, err
			}
			branchNode := newBranchNode(pg.Data())

			for idx := range branchNode.numRecords() {
				childPageId, err := branchNode.childPageId(idx)
				if err != nil {
					return nil, err
				}
				nextLevel = append(nextLevel, childPageId)
			}
			nextLevel = append(nextLevel, branchNode.rightChildPageId())
			mtr.Unpin(nodePageId)
		}
		currentLevel = nextLevel
	}
	return currentLevel, nil
}
