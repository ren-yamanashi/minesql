package btree

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

var (
	ErrDuplicateKey = errors.New("duplicate key")
	ErrKeyNotFound  = errors.New("key not found")
)

type Btree struct {
	bufferPool *buffer.Pool
	MetaPageId page.Id
}

// NewBtree は既存の B+Tree を開く
func NewBtree(bp *buffer.Pool, metaPageId page.Id) *Btree {
	return &Btree{bufferPool: bp, MetaPageId: metaPageId}
}

// CreateBtree は新しい B+Tree を作成する
func CreateBtree(bp *buffer.Pool, fileId page.FileId) (*Btree, error) {
	MetaPageId, err := bp.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}

	// メタページ作成
	_, err = bp.AddPage(MetaPageId)
	if err != nil {
		return nil, err
	}

	pageMeta, err := bp.BufferPageForWrite(MetaPageId)
	if err != nil {
		return nil, err
	}
	metaPage := newMetaPage(pageMeta.Page)

	// ルートリーフノード作成
	rootNodePageId, err := bp.AllocatePageId(MetaPageId.FileId)
	if err != nil {
		return nil, err
	}
	_, err = bp.AddPage(rootNodePageId)
	if err != nil {
		return nil, err
	}
	pageRoot, err := bp.BufferPageForWrite(rootNodePageId)
	if err != nil {
		return nil, err
	}
	rootLeaf := NewLeafNode(pageRoot.Page)
	rootLeaf.Initialize()

	// メタページの設定
	metaPage.setRootPageId(rootNodePageId)
	metaPage.setLeafPageCount(1)
	metaPage.setHeight(1)

	return NewBtree(bp, MetaPageId), nil
}

// LeafPageCount はメタページからリーフページ数を取得する
func (bt *Btree) LeafPageCount() (uint64, error) {
	pageMeta, err := bt.bufferPool.BufferPageForRead(bt.MetaPageId)
	if err != nil {
		return 0, err
	}
	defer bt.bufferPool.UnRefPage(bt.MetaPageId)
	metaPage := newMetaPage(pageMeta.Page)
	return metaPage.leafPageCount(), nil
}

// Height はメタページから B+Tree の高さを取得する
func (bt *Btree) Height() (uint64, error) {
	pageMeta, err := bt.bufferPool.BufferPageForRead(bt.MetaPageId)
	if err != nil {
		return 0, err
	}
	defer bt.bufferPool.UnRefPage(bt.MetaPageId)
	metaPage := newMetaPage(pageMeta.Page)
	return metaPage.height(), nil
}
