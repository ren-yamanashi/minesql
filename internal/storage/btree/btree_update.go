package btree

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
)

// Update は B+Tree の特定のノードの値を更新する
func (t *Tree) Update(record Record) error {
	// メタページを取得
	pageMeta, err := t.bufferPool.PageForRead(t.MetaPageId())
	if err != nil {
		return err
	}
	metaPage := newMetaPage(pageMeta.Data())
	defer t.bufferPool.Unpin(t.MetaPageId())

	// ルートページ取得
	rootPageId := metaPage.rootPageId()
	rootBufPage, err := t.bufferPool.PageForRead(rootPageId)
	if err != nil {
		return err
	}
	defer t.bufferPool.Unpin(rootPageId)
	return t.updateRecursively(rootBufPage, record)
}

// updateRecursively は再帰的にノードを辿ってレコードを更新する
func (t *Tree) updateRecursively(bufPage *buffer.Page, record Record) error {
	pg, err := t.bufferPool.PageForWrite(bufPage.PageId())
	if err != nil {
		return err
	}
	defer t.bufferPool.Unpin(bufPage.PageId())

	nt := nodeType(pg.Data())
	switch nt {
	// ブランチノードの場合: 子ノードに対して再帰実行する
	case nodeTypeBranch:
		branchNode := newBranchNode(pg.Data())
		mode := SearchModeKey{Key: record.Key()}
		childPageId, err := mode.childPageId(branchNode)
		if err != nil {
			return err
		}
		childBufPage, err := t.bufferPool.PageForRead(childPageId)
		if err != nil {
			return err
		}
		defer t.bufferPool.Unpin(childPageId)
		return t.updateRecursively(childBufPage, record)

	// リーフノードの場合: そのまま更新する
	case nodeTypeLeaf:
		leafNode := newLeafNode(pg.Data())
		slotNum, found := leafNode.searchSlotNum(record.Key())
		if !found {
			return ErrKeyNotFound
		}
		if !leafNode.update(slotNum, record) {
			return errors.New("failed to update record")
		}
		return nil

	default:
		return errUnknownNodeType
	}
}
