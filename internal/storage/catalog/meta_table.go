package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type TableMeta struct {
	tree *btree.Tree
}

func NewTableMeta(bp *buffer.Pool, metaPageId page.Id) *TableMeta {
	return &TableMeta{tree: btree.NewTree(bp, metaPageId)}
}

func CreateTableMeta(bp *buffer.Pool) (*TableMeta, error) {
	tree, err := btree.CreateTree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &TableMeta{tree: tree}, nil
}

func (tm *TableMeta) Search(mode SearchMode) (*TableIterator, error) {
	iter, err := tm.tree.Search(mode.Encode())
	if err != nil {
		return nil, err
	}
	return NewTableIterator(iter), nil
}

func (tm *TableMeta) Insert(record TableRecord) error {
	return tm.tree.Insert(record.Encode())
}
