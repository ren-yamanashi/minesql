package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type ColumnMeta struct {
	tree *btree.Tree // カラムメタデータが格納される B+Tree
}

func newColumnMeta(bp *buffer.Pool, metaPageId page.Id) *ColumnMeta {
	return &ColumnMeta{tree: btree.NewTree(bp, metaPageId)}
}

func createColumnMeta(bp *buffer.Pool) (*ColumnMeta, error) {
	tree, err := btree.CreateTree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &ColumnMeta{tree: tree}, nil
}

// Search は指定した検索モードでメタデータを検索し、イテレータを返す
func (cm *ColumnMeta) Search(mode SearchMode) (*columnIterator, error) {
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
	record := newColumnRecord(fileId, name, pos)
	return cm.tree.Insert(record.encode())
}
