package dictionary

import (
	"fmt"

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

// CreateConstraintMeta は制約メタ用の B+Tree を mtr 配下で新規作成する
//   - mtr.Commit / mtr.UnpinAll は呼び出し側で行う
//   - bootstrap 経路のため、途中失敗はすべて panic で扱う
func CreateConstraintMeta(mtr *buffer.Mtr) *ConstraintMeta {
	tree, err := btree.CreateTree(mtr.Pool(), CatalogFileId, mtr)
	if err != nil {
		panic(fmt.Sprintf("dictionary: bootstrap failed to create constraint meta tree: %v", err))
	}
	return &ConstraintMeta{tree: tree}
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
