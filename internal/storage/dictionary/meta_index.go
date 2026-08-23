package dictionary

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type IndexMeta struct {
	tree *btree.Tree
}

func NewIndexMeta(bp *buffer.Pool, metaPageId page.Id) *IndexMeta {
	return &IndexMeta{tree: btree.NewTree(bp, metaPageId)}
}

// CreateIndexMeta はインデックスメタ用の B+Tree を mtr 配下で新規作成する
//   - mtr.Commit / mtr.UnpinAll は呼び出し側で行う
//   - bootstrap 経路のため、途中失敗はすべて panic で扱う
func CreateIndexMeta(mtr *buffer.Mtr) *IndexMeta {
	tree, err := btree.CreateTree(mtr.Pool(), CatalogFileId, mtr)
	if err != nil {
		panic(fmt.Sprintf("dictionary: bootstrap failed to create index meta tree: %v", err))
	}
	return &IndexMeta{tree: tree}
}

func (im *IndexMeta) Search(mtr *buffer.Mtr, mode SearchMode) (*IndexIterator, error) {
	iter, err := im.tree.Search(mtr, mode.Encode())
	if err != nil {
		return nil, err
	}
	return NewIndexIterator(iter), nil
}

func (im *IndexMeta) Insert(mtr *buffer.Mtr, record IndexMetaRecord) error {
	return im.tree.Insert(mtr, record.Encode())
}

func (im *IndexMeta) Delete(mtr *buffer.Mtr, key []byte) error {
	return im.tree.Delete(mtr, key)
}
