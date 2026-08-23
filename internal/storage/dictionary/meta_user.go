package dictionary

import (
	"fmt"

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

// CreateUserMeta はユーザーメタ用の B+Tree を mtr 配下で新規作成する
//   - mtr.Commit / mtr.UnpinAll は呼び出し側で行う
//   - bootstrap 経路のため、途中失敗はすべて panic で扱う
func CreateUserMeta(mtr *buffer.Mtr) *UserMeta {
	tree, err := btree.CreateTree(mtr.Pool(), CatalogFileId, mtr)
	if err != nil {
		panic(fmt.Sprintf("dictionary: bootstrap failed to create user meta tree: %v", err))
	}
	return &UserMeta{tree: tree}
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
