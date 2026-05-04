package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type IndexMeta struct {
	tree *btree.Btree // インデックスメタデータが格納される B+Tree
}

func NewIndexMeta(bp *buffer.BufferPool, metaPageId page.PageId) *IndexMeta {
	return &IndexMeta{tree: btree.NewBtree(bp, metaPageId)}
}

func CreateIndexMeta(bp *buffer.BufferPool) (*IndexMeta, error) {
	tree, err := btree.CreateBtree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &IndexMeta{tree: tree}, nil
}

// Search は指定した検索モードでメタデータを検索し、イテレータを返す
func (im *IndexMeta) Search(mode SearchMode) (*IndexIterator, error) {
	iter, err := im.tree.Search(mode.encode())
	if err != nil {
		return nil, err
	}
	return newIndexIterator(iter), nil
}

// Insert はレコードを挿入する
func (im *IndexMeta) Insert(record IndexRecord) error {
	return im.tree.Insert(record.encode())
}
