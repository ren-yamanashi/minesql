package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type TableMeta struct {
	metaPageId page.PageId
	tree       *btree.Btree // テーブルメタデータが格納される B+Tree
}

func NewTableMeta(bp *buffer.BufferPool, metaPageId page.PageId) *TableMeta {
	tree := btree.NewBtree(bp, metaPageId)
	return &TableMeta{metaPageId: metaPageId, tree: tree}
}

func CreateTableMeta(bp *buffer.BufferPool) (*TableMeta, error) {
	tree, err := btree.CreateBtree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &TableMeta{metaPageId: tree.MetaPageId, tree: tree}, nil
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
//   - fileId: テーブルの FileId
//   - numOfCol: カラム数
func (tm *TableMeta) Insert(name string, fileId page.FileId, numOfCol int) error {
	record := NewTableRecord(fileId, name, numOfCol)
	return tm.tree.Insert(record.encode())
}
