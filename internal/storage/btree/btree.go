package btree

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

var (
	ErrDuplicateKey    = errors.New("duplicate key")
	ErrKeyNotFound     = errors.New("key not found")
	errUnknownNodeType = errors.New("btree: unknown node type")
)

type Tree struct {
	bufferPool *buffer.Pool
	metaPageId page.Id
	latch      *buffer.RWLatch // B+Tree 構造そのものに対する短期排他用のラッチ
}

// NewTree は既存の B+Tree を開く
func NewTree(bp *buffer.Pool, metaPageId page.Id) *Tree {
	return &Tree{
		bufferPool: bp,
		metaPageId: metaPageId,
		latch:      buffer.NewRWLatch(),
	}
}

// CreateTree は新しい B+Tree を作成する
func CreateTree(bp *buffer.Pool, fileId page.FileId) (*Tree, error) {
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()

	metaPageId, err := bp.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}

	// メタページ作成
	_, err = bp.AddPage(metaPageId)
	if err != nil {
		return nil, err
	}

	pageMeta, err := mtr.PageForWrite(metaPageId)
	if err != nil {
		return nil, err
	}
	metaPage := newMetaPage(pageMeta.Data())

	// ルートリーフノード作成
	rootNodePageId, err := bp.AllocatePageId(metaPageId.FileId())
	if err != nil {
		return nil, err
	}
	_, err = bp.AddPage(rootNodePageId)
	if err != nil {
		return nil, err
	}
	pageRoot, err := mtr.PageForWrite(rootNodePageId)
	if err != nil {
		return nil, err
	}
	rootLeaf := newLeafNode(pageRoot.Data())
	rootLeaf.initialize()

	// メタページの設定
	metaPage.setRootPageId(rootNodePageId)
	metaPage.setLeafPageCount(1)
	metaPage.setHeight(1)

	return NewTree(bp, metaPageId), nil
}

// LeafPageCount はメタページからリーフページ数を取得する
func (t *Tree) LeafPageCount() (uint64, error) {
	mtr := buffer.NewMtr(t.bufferPool)
	defer mtr.UnpinAll()
	pageMeta, err := mtr.PageForRead(t.metaPageId)
	if err != nil {
		return 0, err
	}
	metaPage := newMetaPage(pageMeta.Data())
	return metaPage.leafPageCount(), nil
}

// Height はメタページから B+Tree の高さを取得する
func (t *Tree) Height() (uint64, error) {
	mtr := buffer.NewMtr(t.bufferPool)
	defer mtr.UnpinAll()
	pageMeta, err := mtr.PageForRead(t.metaPageId)
	if err != nil {
		return 0, err
	}
	metaPage := newMetaPage(pageMeta.Data())
	return metaPage.height(), nil
}

// MetaPageId は B+Tree のメタページの PageId を返す
func (t *Tree) MetaPageId() page.Id {
	return t.metaPageId
}
