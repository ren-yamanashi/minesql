package dictionary

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

func (cm *ColumnMeta) Search(mtr *buffer.Mtr, mode SearchMode) (*ColumnIterator, error) {
	iter, err := cm.tree.Search(mtr, mode.Encode())
	if err != nil {
		return nil, err
	}
	return NewColumnIterator(iter), nil
}

func (cm *ColumnMeta) Insert(mtr *buffer.Mtr, record ColumnMetaRecord) error {
	return cm.tree.Insert(mtr, record.Encode())
}
