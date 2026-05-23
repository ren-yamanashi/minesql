package btree

import (
	"bytes"
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
	metaPage := newMetaPage(pageMeta.Page)
	defer t.bufferPool.UnRefPage(t.MetaPageId())

	// ルートページ取得
	rootPageId := metaPage.rootPageId()
	rootBufPage, err := t.bufferPool.PageForRead(rootPageId)
	if err != nil {
		return err
	}
	return t.updateRecursively(rootBufPage, record)
}

// updateRecursively は再起的にノードを辿ってレコードを更新する
func (t *Tree) updateRecursively(bufPage *buffer.Page, record Record) error {
	pg, err := t.bufferPool.PageForWrite(bufPage.PageId)
	if err != nil {
		return err
	}

	nt := nodeType(pg.Page)
	switch {
	// ブランチノードの場合: 子ノードに対して再帰実行する
	case bytes.Equal(nt, nodeTypeBranch):
		defer t.bufferPool.UnRefPage(bufPage.PageId)
		branchNode := newBranchNode(pg.Page)
		mode := SearchModeKey{Key: record.Key()}
		childPageId, err := mode.childPageId(branchNode)
		if err != nil {
			return err
		}
		childBufPage, err := t.bufferPool.PageForRead(childPageId)
		if err != nil {
			return err
		}
		return t.updateRecursively(childBufPage, record)

	// リーフノードの場合: そのまま更新する
	case bytes.Equal(nt, nodeTypeLeaf):
		leafNode := newLeafNode(pg.Page)
		slotNum, found := leafNode.searchSlotNum(record.Key())
		if !found {
			return ErrKeyNotFound
		}
		if !leafNode.update(slotNum, record) {
			return errors.New("failed to update record")
		}
		return nil

	default:
		return errors.New("unknown node type")
	}
}
