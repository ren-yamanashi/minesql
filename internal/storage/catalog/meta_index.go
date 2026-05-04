package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type IndexMeta struct {
	metaPageId page.PageId
	tree       *btree.Btree // インデックスメタデータが格納される B+Tree
}

func NewIndexMeta(bp *buffer.BufferPool, metaPageId page.PageId) *IndexMeta {
	tree := btree.NewBtree(bp, metaPageId)
	return &IndexMeta{metaPageId: metaPageId, tree: tree}
}

func CreateIndexMeta(bp *buffer.BufferPool) (*IndexMeta, error) {
	tree, err := btree.CreateBtree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &IndexMeta{metaPageId: tree.MetaPageId, tree: tree}, nil
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
//   - fileId: テーブルの FileId
//   - name: インデックス名
//   - indexId: インデックス ID
//   - indexType: インデックス種類
//   - numOfCol: インデックスを構成するカラム数
func (im *IndexMeta) Insert(fileId page.FileId, name string, indexId IndexId, indexType IndexType, numOfCol int) error {
	record := NewIndexRecord(fileId, indexId, name, indexType, numOfCol)
	return im.tree.Insert(record.encode())
}
