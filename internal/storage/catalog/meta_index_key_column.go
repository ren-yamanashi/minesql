package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type IndexKeyColMeta struct {
	metaPageId page.PageId
	tree       *btree.Btree // インデックスキーカラムメタデータが格納される B+Tree
}

func NewIndexKeyColMeta(bp *buffer.BufferPool, metaPageId page.PageId) *IndexKeyColMeta {
	tree := btree.NewBtree(bp, metaPageId)
	return &IndexKeyColMeta{metaPageId: metaPageId, tree: tree}
}

func CreateIndexKeyColMeta(bp *buffer.BufferPool) (*IndexKeyColMeta, error) {
	tree, err := btree.CreateBtree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &IndexKeyColMeta{metaPageId: tree.MetaPageId, tree: tree}, nil
}

// Search は指定した検索モードでメタデータを検索し、イテレータを返す
func (kcm *IndexKeyColMeta) Search(mode SearchMode) (*IndexKeyColIterator, error) {
	iter, err := kcm.tree.Search(mode.encode())
	if err != nil {
		return nil, err
	}
	return newIndexKeyColIterator(iter), nil
}

// Insert はレコードを挿入する
//   - indexId: インデックス ID
//   - name: カラム名
//   - pos: テーブル上のカラム位置
func (kcm *IndexKeyColMeta) Insert(indexId IndexId, name string, colPos int) error {
	record := newIndexKeyColRecord(indexId, name, colPos)
	return kcm.tree.Insert(record.encode())
}
