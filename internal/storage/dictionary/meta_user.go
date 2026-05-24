package dictionary

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type UserMeta struct {
	tree *btree.Tree
}

func NewUserMeta(bp *buffer.Pool, metaPageId page.Id) *UserMeta {
	return &UserMeta{tree: btree.NewTree(bp, metaPageId)}
}

func CreateUserMeta(bp *buffer.Pool) (*UserMeta, error) {
	tree, err := btree.CreateTree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &UserMeta{tree: tree}, nil
}

func (um *UserMeta) Search(mode SearchMode) (*UserIterator, error) {
	iter, err := um.tree.Search(mode.Encode())
	if err != nil {
		return nil, err
	}
	return NewUserIterator(iter), nil
}

func (um *UserMeta) Insert(record UserMetaRecord) error {
	return um.tree.Insert(record.Encode())
}
