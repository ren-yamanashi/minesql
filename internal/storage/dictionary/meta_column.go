package dictionary

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

type ColumnMeta struct {
	tree *btree.Tree
}

func NewColumnMeta(bp *buffer.Pool, metaPageId page.Id) *ColumnMeta {
	return &ColumnMeta{tree: btree.NewTree(bp, metaPageId)}
}

func CreateColumnMeta(bp *buffer.Pool, redoLog *redo.Buffer) (*ColumnMeta, error) {
	tree, err := btree.CreateTree(bp, catalogFileId, redoLog, lock.SystemReservedTrxId)
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
