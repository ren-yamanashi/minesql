package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type IndexMeta struct {
	tree *btree.Tree
}

func NewIndexMeta(bp *buffer.Pool, metaPageId page.Id) *IndexMeta {
	return &IndexMeta{tree: btree.NewTree(bp, metaPageId)}
}

func CreateIndexMeta(bp *buffer.Pool) (*IndexMeta, error) {
	tree, err := btree.CreateTree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &IndexMeta{tree: tree}, nil
}

func (im *IndexMeta) Search(mode SearchMode) (*IndexIterator, error) {
	iter, err := im.tree.Search(mode.Encode())
	if err != nil {
		return nil, err
	}
	return NewIndexIterator(iter), nil
}

func (im *IndexMeta) Insert(record IndexRecord) error {
	return im.tree.Insert(record.Encode())
}
