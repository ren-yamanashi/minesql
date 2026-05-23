package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type ConstraintMeta struct {
	tree *btree.Tree // 制約メタデータが格納される B+Tree
}

func newConstraintMeta(bp *buffer.Pool, metaPageId page.Id) *ConstraintMeta {
	return &ConstraintMeta{tree: btree.NewTree(bp, metaPageId)}
}

func createConstraintMeta(bp *buffer.Pool) (*ConstraintMeta, error) {
	tree, err := btree.CreateTree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &ConstraintMeta{tree: tree}, nil
}

// Search は指定した検索モードでメタデータを検索し、イテレータを返す
func (cm *ConstraintMeta) Search(mode SearchMode) (*constraintIterator, error) {
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
