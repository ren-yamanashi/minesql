package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type ColumnMeta struct {
	metaPageId page.PageId
	tree       *btree.Btree // カラムメタデータが格納される B+Tree
}

func NewColumnMeta(bp *buffer.BufferPool, metaPageId page.PageId) *ColumnMeta {
	tree := btree.NewBtree(bp, metaPageId)
	return &ColumnMeta{metaPageId: metaPageId, tree: tree}
}

func CreateColumnMeta(bp *buffer.BufferPool) (*ColumnMeta, error) {
	tree, err := btree.CreateBtree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &ColumnMeta{metaPageId: tree.MetaPageId, tree: tree}, nil
}

// Search は指定した検索モードでメタデータを検索し、イテレータを返す
func (cm *ColumnMeta) Search(mode SearchMode) (*ColumnIterator, error) {
	iter, err := cm.tree.Search(mode.encode())
	if err != nil {
		return nil, err
	}
	return newColumnIterator(iter), nil
}

// Insert はレコードを挿入する
//   - fileId: テーブルの FileId
//   - name: カラム名
//   - pos: テーブル上のカラム位置
func (cm *ColumnMeta) Insert(fileId page.FileId, name string, pos int) error {
	record := NewColumnRecord(fileId, name, pos)
	return cm.tree.Insert(record.encode())
}
