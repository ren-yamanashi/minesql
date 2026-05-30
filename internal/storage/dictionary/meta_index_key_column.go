package dictionary

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type IndexKeyColumnMeta struct {
	tree *btree.Tree
}

func NewIndexKeyColumnMeta(bp *buffer.Pool, metaPageId page.Id) *IndexKeyColumnMeta {
	return &IndexKeyColumnMeta{tree: btree.NewTree(bp, metaPageId)}
}

func CreateIndexKeyColumnMeta(bp *buffer.Pool) (*IndexKeyColumnMeta, error) {
	tree, err := btree.CreateTree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &IndexKeyColumnMeta{tree: tree}, nil
}

func (kcm *IndexKeyColumnMeta) Search(mtr *buffer.Mtr, mode SearchMode) (*IndexKeyColumnIterator, error) {
	iter, err := kcm.tree.Search(mtr, mode.Encode())
	if err != nil {
		return nil, err
	}
	return NewIndexKeyColumnIterator(iter), nil
}

func (kcm *IndexKeyColumnMeta) Insert(mtr *buffer.Mtr, record IndexKeyColumnMetaRecord) error {
	return kcm.tree.Insert(mtr, record.Encode())
}
