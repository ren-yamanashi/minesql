package btree

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// Update は B+Tree の特定のノードの値を更新する
//
// record.KeyBytes() で対象のリーフノードを特定し、record.NonKeyBytes() で値を上書きする
func (bt *BTree) Update(bp *buffer.BufferPool, record node.Record) error {
	// メタページを取得
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	metaData, err := bp.GetReadPageData(bt.MetaPageId)
	if err != nil {
		return err
	}
	meta := newMetaPage(page.NewPage(metaData))

	// ルートページを取得
	rootPageId := meta.rootPageId()
	rootPageBuf, err := bp.FetchPage(rootPageId)
	if err != nil {
		return err
	}

	return bt.updateRecursively(bp, rootPageBuf, record)
}

// updateRecursively は再帰的にノードを辿って該当のリーフノードを見つけ、レコードを更新する
func (bt *BTree) updateRecursively(bp *buffer.BufferPool, nodeBuffer *buffer.BufferPage, record node.Record) error {
	nodeData, err := bp.GetReadPageData(nodeBuffer.PageId)
	if err != nil {
		return err
	}
	nodeType := node.GetNodeType(page.NewPage(nodeData).Body)

	switch {
	case bytes.Equal(nodeType, node.NodeTypeBranch):
		// ブランチノードの場合、再帰呼び出し後に UnRefPage を呼び出す
		defer bp.UnRefPage(nodeBuffer.PageId)

		branch := node.NewBranch(page.NewPage(nodeData).Body)

		// record.KeyBytes() を使って子ノードを特定
		searchMode := SearchModeKey{Key: record.KeyBytes()}
		childPageId := searchMode.childPageId(branch)
		childNodeBuffer, err := bp.FetchPage(childPageId)
		if err != nil {
			return err
		}

		// 再帰呼び出し
		return bt.updateRecursively(bp, childNodeBuffer, record)

	case bytes.Equal(nodeType, node.NodeTypeLeaf):
		nodeWriteData, err := bp.GetWritePageData(nodeBuffer.PageId)
		if err != nil {
			return err
		}
		leaf := node.NewLeaf(page.NewPage(nodeWriteData).Body)

		// 該当のキーを持つレコードを見つける
		slotNum, found := leaf.SearchSlotNum(record.KeyBytes())
		if !found {
			return ErrKeyNotFound
		}

		// レコードの値を新しい値に更新
		if !leaf.Update(slotNum, record) {
			return errors.New("failed to update record")
		}
		return nil

	default:
		panic("unknown node type")
	}
}
