package dictionary

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type IndexKeyColumnMeta struct {
	tree *btree.Tree
}

func NewIndexKeyColumnMeta(bp *buffer.Pool, metaPageId page.Id) *IndexKeyColumnMeta {
	return &IndexKeyColumnMeta{tree: btree.NewTree(bp, metaPageId)}
}

// CreateIndexKeyColumnMeta はインデックスキーカラムメタ用の B+Tree を mtr 配下で新規作成する
//   - mtr.Commit / mtr.UnpinAll は呼び出し側で行う
//   - bootstrap 経路のため、途中失敗はすべて panic で扱う
func CreateIndexKeyColumnMeta(mtr *buffer.Mtr) *IndexKeyColumnMeta {
	tree, err := btree.CreateTree(mtr.Pool(), CatalogFileId, mtr)
	if err != nil {
		panic(fmt.Sprintf("dictionary: bootstrap failed to create index key column meta tree: %v", err))
	}
	return &IndexKeyColumnMeta{tree: tree}
}

func (kcm *IndexKeyColumnMeta) Search(mtr *buffer.Mtr, mode SearchMode) (*IndexKeyColumnIterator, error) {
	iter, err := kcm.tree.Search(mtr, mode.Encode())
	if err != nil {
		return nil, err
	}
	return NewIndexKeyColumnIterator(iter), nil
}

func (kcm *IndexKeyColumnMeta) Insert(mtr *buffer.Mtr, record IndexKeyColumnMetaRecord) error {
	return kcm.tree.Insert(mtr, record.Encode())
}

func (kcm *IndexKeyColumnMeta) Delete(mtr *buffer.Mtr, key []byte) error {
	return kcm.tree.Delete(mtr, key)
}
