package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type ConstraintMeta struct {
	tree *btree.Btree // 制約メタデータが格納される B+Tree
}

func NewConstraintMeta(bp *buffer.BufferPool, metaPageId page.PageId) *ConstraintMeta {
	return &ConstraintMeta{tree: btree.NewBtree(bp, metaPageId)}
}

func CreateConstraintMeta(bp *buffer.BufferPool) (*ConstraintMeta, error) {
	tree, err := btree.CreateBtree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &ConstraintMeta{tree: tree}, nil
}

// Search は指定した検索モードでメタデータを検索し、イテレータを返す
func (cm *ConstraintMeta) Search(mode SearchMode) (*ConstraintIterator, error) {
	iter, err := cm.tree.Search(mode.encode())
	if err != nil {
		return nil, err
	}
	return newConstraintIterator(iter), nil
}

// Insert はレコードを挿入する
func (cm *ConstraintMeta) Insert(record ConstraintRecord) error {
	return cm.tree.Insert(record.encode())
}
