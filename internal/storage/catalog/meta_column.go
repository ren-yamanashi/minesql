package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type ColumnMeta struct {
	tree *btree.Tree
}

func NewColumnMeta(bp *buffer.Pool, metaPageId page.Id) *ColumnMeta {
	return &ColumnMeta{tree: btree.NewTree(bp, metaPageId)}
}

func CreateColumnMeta(bp *buffer.Pool) (*ColumnMeta, error) {
	tree, err := btree.CreateTree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &ColumnMeta{tree: tree}, nil
}

func (cm *ColumnMeta) Search(mode SearchMode) (*ColumnIterator, error) {
	iter, err := cm.tree.Search(mode.Encode())
	if err != nil {
		return nil, err
	}
	return NewColumnIterator(iter), nil
}

func (cm *ColumnMeta) Insert(record ColumnRecord) error {
	return cm.tree.Insert(record.Encode())
}
