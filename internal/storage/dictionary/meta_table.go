package dictionary

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type TableMeta struct {
	tree *btree.Btree // テーブルメタデータが格納される B+Tree
}

func NewTableMeta(bp *buffer.BufferPool, metaPageId page.PageId) *TableMeta {
	tree := btree.NewBtree(bp, metaPageId)
	return &TableMeta{tree: tree}
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
//   - fileId: テーブルの FileId
//   - name: テーブル名
//   - numOfCol: カラム数
func (tm *TableMeta) Insert(fileId page.FileId, name string, numOfCol int) error {
	record := newTableRecord(fileId, name, numOfCol)
	return tm.tree.Insert(record.encode())
}
