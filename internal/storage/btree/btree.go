package btree

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

var (
	ErrDuplicateKey    = errors.New("duplicate key")
	ErrKeyNotFound     = errors.New("key not found")
	errUnknownNodeType = errors.New("btree: unknown node type")
	errRecordTooLarge  = errors.New("btree: record exceeds max record size")
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
//   - mtr: メタページ・ルートリーフ初期化を記録する書き込み Mtr。Commit は呼び出し側
func CreateTree(bp *buffer.Pool, fileId page.FileId, mtr *buffer.Mtr) (*Tree, error) {
	metaPageId, err := fsp.AllocatePage(mtr, fileId)
	if err != nil {
		return nil, err
	}

	// メタページ作成
	if _, err := bp.AddPage(metaPageId); err != nil {
		return nil, err
	}

	pageMeta, err := mtr.PageForWrite(metaPageId)
	if err != nil {
		return nil, err
	}
	metaPage := newMetaPage(pageMeta)

	// ルートリーフノード作成
	rootNodePageId, err := fsp.AllocatePage(mtr, metaPageId.FileId())
	if err != nil {
		return nil, err
	}
	if _, err := bp.AddPage(rootNodePageId); err != nil {
		return nil, err
	}
	pageRoot, err := mtr.PageForWrite(rootNodePageId)
	if err != nil {
		return nil, err
	}
	rootLeaf := newLeafNode(pageRoot)
	rootLeaf.initialize()

	// メタページの設定
	metaPage.setRootPageId(rootNodePageId)
	metaPage.setLeafPageCount(1)
	metaPage.setHeight(1)

	return NewTree(bp, metaPageId), nil
}

// LeafPageCount はメタページからリーフページ数を取得する
//   - mtr: メタページの S-latch / Pin を保持する mini-transaction。 解放は呼び出し側
func (t *Tree) LeafPageCount(mtr *buffer.Mtr) (uint64, error) {
	pageMeta, err := mtr.PageForRead(t.metaPageId)
	if err != nil {
		return 0, err
	}
	metaPage := newMetaPage(pageMeta)
	return metaPage.leafPageCount(), nil
}

// Height はメタページから B+Tree の高さを取得する
//   - mtr: メタページの S-latch / Pin を保持する mini-transaction。 解放は呼び出し側
func (t *Tree) Height(mtr *buffer.Mtr) (uint64, error) {
	pageMeta, err := mtr.PageForRead(t.metaPageId)
	if err != nil {
		return 0, err
	}
	metaPage := newMetaPage(pageMeta)
	return metaPage.height(), nil
}

// MetaPageId は B+Tree のメタページの PageId を返す
func (t *Tree) MetaPageId() page.Id {
	return t.metaPageId
}
