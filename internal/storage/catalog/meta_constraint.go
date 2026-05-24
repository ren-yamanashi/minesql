package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type ConstraintMeta struct {
	tree *btree.Tree
}

func NewConstraintMeta(bp *buffer.Pool, metaPageId page.Id) *ConstraintMeta {
	return &ConstraintMeta{tree: btree.NewTree(bp, metaPageId)}
}

func CreateConstraintMeta(bp *buffer.Pool) (*ConstraintMeta, error) {
	tree, err := btree.CreateTree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &ConstraintMeta{tree: tree}, nil
}

func (cm *ConstraintMeta) Search(mode SearchMode) (*ConstraintIterator, error) {
	iter, err := cm.tree.Search(mode.Encode())
	if err != nil {
		return nil, err
	}
	return NewConstraintIterator(iter), nil
}

func (cm *ConstraintMeta) Insert(record ConstraintRecord) error {
	return cm.tree.Insert(record.Encode())
}
