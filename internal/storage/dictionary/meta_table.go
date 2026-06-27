package dictionary

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type TableMeta struct {
	tree *btree.Tree
}

func NewTableMeta(bp *buffer.Pool, metaPageId page.Id) *TableMeta {
	return &TableMeta{tree: btree.NewTree(bp, metaPageId)}
}

// CreateTableMeta はテーブルメタ用の B+Tree を mtr 配下で新規作成する
//   - mtr.Commit / mtr.UnpinAll は呼び出し側で行う
func CreateTableMeta(mtr *buffer.Mtr) (*TableMeta, error) {
	tree, err := btree.CreateTree(mtr.Pool(), CatalogFileId, mtr)
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

func (tm *TableMeta) Delete(mtr *buffer.Mtr, key []byte) error {
	return tm.tree.Delete(mtr, key)
}
