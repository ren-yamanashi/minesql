package dictionary

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type IndexKeyColMeta struct {
	tree *btree.Btree // インデックスキーカラムメタデータが格納される B+Tree
}

func NewIndexKeyColMeta(bp *buffer.BufferPool, metaPageId page.PageId) *IndexKeyColMeta {
	tree := btree.NewBtree(bp, metaPageId)
	return &IndexKeyColMeta{tree: tree}
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
