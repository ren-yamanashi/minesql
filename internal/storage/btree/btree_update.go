package btree

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
)

// Update は B+Tree の特定のノードの値を更新する
func (bt *Btree) Update(record node.Record) error {
	// メタページを取得
	pageMeta, err := bt.bufferPool.GetReadPage(bt.metaPageId)
	if err != nil {
		return err
	}
	metaPage := newMetaPage(pageMeta)
	defer bt.bufferPool.UnRefPage(bt.metaPageId)

	// ルートページ取得
	rootPageId := metaPage.rootPageId()
	rootBufPage, err := bt.bufferPool.FetchPage(rootPageId)
	if err != nil {
		return err
	}
	return bt.updateRecursively(rootBufPage, record)
}

// updateRecursively は再起的にノードを辿ってレコードを更新する
func (bt *Btree) updateRecursively(bufPage *buffer.BufferPage, record node.Record) error {
	pg, err := bt.bufferPool.GetWritePage(bufPage.PageId)
	if err != nil {
		return err
	}

	nodeType := node.GetNodeType(pg)
	switch {
	// ブランチノードの場合: 子ノードに対して再帰実行する
	case bytes.Equal(nodeType, node.NodeTypeBranch):
		defer bt.bufferPool.UnRefPage(bufPage.PageId)
		branchNode := node.NewBranchNode(pg)
		mode := SearchModeKey{Key: record.Key()}
		childPageId, err := mode.childPageId(branchNode)
		if err != nil {
			return err
		}
		childBufPage, err := bt.bufferPool.FetchPage(childPageId)
		if err != nil {
			return err
		}
		return bt.updateRecursively(childBufPage, record)

	// リーフノードの場合: そのまま更新する
	case bytes.Equal(nodeType, node.NodeTypeLeaf):
		leafNode := node.NewLeafNode(pg)
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
