package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type IndexKeyColumnMeta struct {
	tree *btree.Tree // インデックスキーカラムメタデータが格納される B+Tree
}

func NewIndexKeyColumnMeta(bp *buffer.Pool, metaPageId page.Id) *IndexKeyColumnMeta {
	return &IndexKeyColumnMeta{tree: btree.NewTree(bp, metaPageId)}
}

func CreateIndexKeyColumnMeta(bp *buffer.Pool) (*IndexKeyColumnMeta, error) {
	tree, err := btree.CreateTree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &IndexKeyColumnMeta{tree: tree}, nil
}

// Search は指定した検索モードでメタデータを検索し、イテレータを返す
func (kcm *IndexKeyColumnMeta) Search(mode SearchMode) (*IndexKeyColumnIterator, error) {
	iter, err := kcm.tree.Search(mode.encode())
	if err != nil {
		return nil, err
	}
	return NewIndexKeyColumnIterator(iter), nil
}

// Insert はレコードを挿入する
//   - indexId: インデックス ID
//   - name: カラム名
//   - colPos: インデックス上のカラム位置
func (kcm *IndexKeyColumnMeta) Insert(indexId IndexId, name string, colPos int) error {
	record := NewIndexKeyColumnRecord(indexId, name, colPos)
	return kcm.tree.Insert(record.encode())
}
