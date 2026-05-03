package btree

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

var (
	ErrDuplicateKey = errors.New("duplicate key")
	ErrKeyNotFound  = errors.New("key not found")
)

type Btree struct {
	bufferPool *buffer.BufferPool
	MetaPageId page.PageId
}

// NewBtree は既存の B+Tree を開く
func NewBtree(bp *buffer.BufferPool, metaPageId page.PageId) *Btree {
	return &Btree{bufferPool: bp, MetaPageId: metaPageId}
}

// CreateBtree は新しい B+Tree を作成する
func CreateBtree(bp *buffer.BufferPool, fileId page.FileId) (*Btree, error) {
	metaPageId, err := bp.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}

	// メタページ作成
	_, err = bp.AddPage(metaPageId)
	if err != nil {
		return nil, err
	}

	pageMeta, err := bp.GetWritePage(metaPageId)
	if err != nil {
		return nil, err
	}
	metaPage := newMetaPage(pageMeta)

	// ルートリーフノード作成
	rootNodePageId, err := bp.AllocatePageId(metaPageId.FileId)
	if err != nil {
		return nil, err
	}
	_, err = bp.AddPage(rootNodePageId)
	if err != nil {
		return nil, err
	}
	pageRoot, err := bp.GetWritePage(rootNodePageId)
	if err != nil {
		return nil, err
	}
	rootLeaf := node.NewLeafNode(pageRoot)
	rootLeaf.Initialize()

	// メタページの設定
	metaPage.setRootPageId(rootNodePageId)
	metaPage.setLeafPageCount(1)
	metaPage.setHeight(1)

	return NewBtree(bp, metaPageId), nil
}

// LeafPageCount はメタページからリーフページ数を取得する
func (bt *Btree) LeafPageCount() (uint64, error) {
	pageMeta, err := bt.bufferPool.GetReadPage(bt.MetaPageId)
	if err != nil {
		return 0, err
	}
	defer bt.bufferPool.UnRefPage(bt.MetaPageId)
	metaPage := newMetaPage(pageMeta)
	return metaPage.leafPageCount(), nil
}

// Height はメタページから B+Tree の高さを取得する
func (bt *Btree) Height() (uint64, error) {
	pageMeta, err := bt.bufferPool.GetReadPage(bt.MetaPageId)
	if err != nil {
		return 0, err
	}
	defer bt.bufferPool.UnRefPage(bt.MetaPageId)
	metaPage := newMetaPage(pageMeta)
	return metaPage.height(), nil
}
