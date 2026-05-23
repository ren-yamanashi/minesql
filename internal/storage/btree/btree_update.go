package btree

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
)

// Update は B+Tree の特定のノードの値を更新する
func (bt *Btree) Update(record Record) error {
	// メタページを取得
	pageMeta, err := bt.bufferPool.BufferPageForRead(bt.MetaPageId)
	if err != nil {
		return err
	}
	metaPage := newMetaPage(pageMeta.Page)
	defer bt.bufferPool.UnRefPage(bt.MetaPageId)

	// ルートページ取得
	rootPageId := metaPage.rootPageId()
	rootBufPage, err := bt.bufferPool.BufferPageForRead(rootPageId)
	if err != nil {
		return err
	}
	return bt.updateRecursively(rootBufPage, record)
}

// updateRecursively は再起的にノードを辿ってレコードを更新する
func (bt *Btree) updateRecursively(bufPage *buffer.Page, record Record) error {
	pg, err := bt.bufferPool.BufferPageForWrite(bufPage.PageId)
	if err != nil {
		return err
	}

	nodeType := GetNodeType(pg.Page)
	switch {
	// ブランチノードの場合: 子ノードに対して再帰実行する
	case bytes.Equal(nodeType, NodeTypeBranch):
		defer bt.bufferPool.UnRefPage(bufPage.PageId)
		branchNode := NewBranchNode(pg.Page)
		mode := SearchModeKey{Key: record.Key()}
		childPageId, err := mode.childPageId(branchNode)
		if err != nil {
			return err
		}
		childBufPage, err := bt.bufferPool.BufferPageForRead(childPageId)
		if err != nil {
			return err
		}
		return bt.updateRecursively(childBufPage, record)

	// リーフノードの場合: そのまま更新する
	case bytes.Equal(nodeType, NodeTypeLeaf):
		leafNode := NewLeafNode(pg.Page)
		slotNum, found := leafNode.SearchSlotNum(record.Key())
		if !found {
			return ErrKeyNotFound
		}
		if !leafNode.Update(slotNum, record) {
			return errors.New("failed to update record")
		}
		return nil

	default:
		return errors.New("unknown node type")
	}
}
