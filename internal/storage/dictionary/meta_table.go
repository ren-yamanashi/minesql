package dictionary

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

type TableMeta struct {
	tree *btree.Tree
}

func NewTableMeta(bp *buffer.Pool, metaPageId page.Id) *TableMeta {
	return &TableMeta{tree: btree.NewTree(bp, metaPageId)}
}

func CreateTableMeta(bp *buffer.Pool, redoLog *redo.Buffer) (*TableMeta, error) {
	tree, err := btree.CreateTree(bp, catalogFileId, redoLog, lock.SystemReservedTrxId)
	if err != nil {
		return nil, err
	}
	return &TableMeta{tree: tree}, nil
}

func (tm *TableMeta) Search(mtr *buffer.Mtr, mode SearchMode) (*TableIterator, error) {
	iter, err := tm.tree.Search(mtr, mode.Encode())
	if err != nil {
		return nil, err
	}
	return NewTableIterator(iter), nil
}

func (tm *TableMeta) Insert(mtr *buffer.Mtr, record TableMetaRecord) error {
	return tm.tree.Insert(mtr, record.Encode())
}
