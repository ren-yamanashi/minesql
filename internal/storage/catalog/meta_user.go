package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type UserMeta struct {
	tree *btree.Tree // ユーザーメタデータが格納される B+Tree
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

// Search は指定した検索モードでメタデータを検索し、イテレータを返す
func (um *UserMeta) Search(mode SearchMode) (*UserIterator, error) {
	iter, err := um.tree.Search(mode.encode())
	if err != nil {
		return nil, err
	}
	return NewUserIterator(iter), nil
}

// Insert はレコードを挿入する
func (um *UserMeta) Insert(record UserRecord) error {
	return um.tree.Insert(record.encode())
}
