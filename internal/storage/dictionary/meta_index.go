package dictionary

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

type IndexMeta struct {
	tree *btree.Tree
}

func NewIndexMeta(bp *buffer.Pool, metaPageId page.Id) *IndexMeta {
	return &IndexMeta{tree: btree.NewTree(bp, metaPageId)}
}

func CreateIndexMeta(bp *buffer.Pool, redoLog *redo.Buffer) (*IndexMeta, error) {
	mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
	tree, err := btree.CreateTree(bp, catalogFileId, mtr)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	if err := mtr.Commit(); err != nil {
		return nil, err
	}
	return &IndexMeta{tree: tree}, nil
}

func (im *IndexMeta) Search(mtr *buffer.Mtr, mode SearchMode) (*IndexIterator, error) {
	iter, err := im.tree.Search(mtr, mode.Encode())
	if err != nil {
		return nil, err
	}
	return NewIndexIterator(iter), nil
}

func (im *IndexMeta) Insert(mtr *buffer.Mtr, record IndexMetaRecord) error {
	return im.tree.Insert(mtr, record.Encode())
}

func (im *IndexMeta) Delete(mtr *buffer.Mtr, key []byte) error {
	return im.tree.Delete(mtr, key)
}
