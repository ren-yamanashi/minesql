package dictionary

import (
	"fmt"

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

// CreateColumnMeta はカラムメタ用の B+Tree を mtr 配下で新規作成する
//   - mtr.Commit / mtr.UnpinAll は呼び出し側で行う
//   - bootstrap 経路のため、途中失敗はすべて panic で扱う
func CreateColumnMeta(mtr *buffer.Mtr) *ColumnMeta {
	tree, err := btree.CreateTree(mtr.Pool(), CatalogFileId, mtr)
	if err != nil {
		panic(fmt.Sprintf("dictionary: bootstrap failed to create column meta tree: %v", err))
	}
	return &ColumnMeta{tree: tree}
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

func (cm *ColumnMeta) Delete(mtr *buffer.Mtr, key []byte) error {
	return cm.tree.Delete(mtr, key)
}
