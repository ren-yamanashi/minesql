package dictionary

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

type UserMeta struct {
	tree *btree.Tree
}

func NewUserMeta(bp *buffer.Pool, metaPageId page.Id) *UserMeta {
	return &UserMeta{tree: btree.NewTree(bp, metaPageId)}
}

func CreateUserMeta(bp *buffer.Pool, redoLog *redo.Buffer) (*UserMeta, error) {
	mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
	tree, err := btree.CreateTree(bp, catalogFileId, mtr)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	if err := mtr.Commit(); err != nil {
		return nil, err
	}
	return &UserMeta{tree: tree}, nil
}

func (um *UserMeta) Search(mtr *buffer.Mtr, mode SearchMode) (*UserIterator, error) {
	iter, err := um.tree.Search(mtr, mode.Encode())
	if err != nil {
		return nil, err
	}
	return NewUserIterator(iter), nil
}

func (um *UserMeta) Insert(mtr *buffer.Mtr, record UserMetaRecord) error {
	return um.tree.Insert(mtr, record.Encode())
}
