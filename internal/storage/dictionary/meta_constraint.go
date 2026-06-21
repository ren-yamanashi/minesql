package dictionary

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

type ConstraintMeta struct {
	tree *btree.Tree
}

func NewConstraintMeta(bp *buffer.Pool, metaPageId page.Id) *ConstraintMeta {
	return &ConstraintMeta{tree: btree.NewTree(bp, metaPageId)}
}

func CreateConstraintMeta(bp *buffer.Pool, redoLog *redo.Buffer) (*ConstraintMeta, error) {
	mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
	tree, err := btree.CreateTree(bp, catalogFileId, mtr)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	if err := mtr.Commit(); err != nil {
		return nil, err
	}
	return &ConstraintMeta{tree: tree}, nil
}

func (cm *ConstraintMeta) Search(mtr *buffer.Mtr, mode SearchMode) (*ConstraintIterator, error) {
	iter, err := cm.tree.Search(mtr, mode.Encode())
	if err != nil {
		return nil, err
	}
	return NewConstraintIterator(iter), nil
}

func (cm *ConstraintMeta) Insert(mtr *buffer.Mtr, record ConstraintMetaRecord) error {
	return cm.tree.Insert(mtr, record.Encode())
}

func (cm *ConstraintMeta) Delete(mtr *buffer.Mtr, key []byte) error {
	return cm.tree.Delete(mtr, key)
}
