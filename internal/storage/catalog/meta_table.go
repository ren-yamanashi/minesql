package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type TableMeta struct {
	tree *btree.Btree // テーブルメタデータが格納される B+Tree
}

func NewTableMeta(bp *buffer.BufferPool, metaPageId page.PageId) *TableMeta {
	return &TableMeta{tree: btree.NewBtree(bp, metaPageId)}
}

func CreateTableMeta(bp *buffer.BufferPool) (*TableMeta, error) {
	tree, err := btree.CreateBtree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &TableMeta{tree: tree}, nil
}

// Search は指定した検索モードでメタデータを検索し、イテレータを返す
func (tm *TableMeta) Search(mode SearchMode) (*TableIterator, error) {
	iter, err := tm.tree.Search(mode.encode())
	if err != nil {
		return nil, err
	}
	return newTableIterator(iter), nil
}

// Insert はレコードを挿入する
//   - name: テーブル名
//   - metaPageId: プライマリインデックスの B+Tree メタページ ID
//   - numOfCol: カラム数
func (tm *TableMeta) Insert(name string, metaPageId page.PageId, numOfCol int) error {
	record := newTableRecord(name, metaPageId, numOfCol)
	return tm.tree.Insert(record.encode())
}
