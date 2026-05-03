package btree

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
)

// Delete は B+Tree からレコードを削除する
func (bt *Btree) Delete(key []byte) error {
	// メタページを取得
	pageMeta, err := bt.bufferPool.GetWritePage(bt.MetaPageId)
	if err != nil {
		return err
	}
	defer bt.bufferPool.UnRefPage(bt.MetaPageId)
	metaPage := newMetaPage(pageMeta)

	// ルートページを取得
	rootPageId := metaPage.rootPageId()
	rootPageBuf, err := bt.bufferPool.FetchPage(rootPageId)
	if err != nil {
		return err
	}

	// 再帰的に削除
	underflow, isLeafMerged, err := bt.deleteRecursively(rootPageBuf, key)
	if err != nil {
		return err
	}

	// ルートノードがブランチノードで、子が 1 つになった場合 (=ブランチノード1, リーフノード1 になった場合)、子をルートにする
	var isRootCollapsed bool
	pageRoot, err := bt.bufferPool.GetReadPage(rootPageBuf.PageId)
	if err != nil {
		return err
	}
	if underflow && bytes.Equal(node.GetNodeType(pageRoot), node.NodeTypeBranch) {
		branch := node.NewBranchNode(pageRoot)
		if branch.NumRecords() == 0 {
			isRootCollapsed = true
		}
	}

	// リーフマージもルート縮退も発生しなかった場合
	if !isLeafMerged && !isRootCollapsed {
		return nil
	}

	// リーフマージが発生した場合
	if isLeafMerged {
		metaPage.setLeafPageCount(metaPage.leafPageCount() - 1)
	}
	if !isRootCollapsed {
		return nil
	}

	// ルートノードの縮退が発生した場合
	branchNode := node.NewBranchNode(pageRoot)
	newRootPageId := branchNode.RightChildPageId()
	metaPage.setRootPageId(newRootPageId)
	metaPage.setHeight(metaPage.height() - 1)
	return nil
}

// deleteRecursively は再帰的にノードを辿ってレコードを削除する
//   - bufPage: 削除先のノードのバッファページ
//   - key: 削除するキー
//   - return:
//   - underflow: アンダーフローが発生したか
//   - isLeafMerged: リーフノードのマージが発生したか
func (bt *Btree) deleteRecursively(bufPage *buffer.BufferPage, key []byte) (underflow bool, isLeafMerged bool, err error) {
	pg, err := bt.bufferPool.GetWritePage(bufPage.PageId)
	if err != nil {
		return false, false, err
	}
	nodeType := node.GetNodeType(pg)

	switch {
	// ブランチノードの場合: 子ノードに対して再帰実行する
	case bytes.Equal(nodeType, node.NodeTypeBranch):
		// 削除先の子ノードを取得
		branchNode := node.NewBranchNode(pg)
		childSlotNum, found := branchNode.SearchSlotNum(key)
		if found {
			childSlotNum++ // 境界キーと一致する場合、右の子に属する
		}
		childPageId, err := branchNode.ChildPageId(childSlotNum)
		if err != nil {
			return false, false, err
		}
		childBufPage, err := bt.bufferPool.FetchPage(childPageId)
		if err != nil {
			return false, false, err
		}
		defer bt.bufferPool.UnRefPage(childPageId)

		// 子ノードに対して削除処理を再帰的に実行
		underflow, isLeafMerged, err := bt.deleteRecursively(childBufPage, key)
		if err != nil {
			return false, false, err
		}
		// 子ノードがアンダーフローしなかった場合、終了
		if !underflow {
			return false, isLeafMerged, nil
		}
		// 子ノードがアンダーフローした場合、兄弟ノードとマージ
		uf, lm, err := bt.deleteUnderflow(branchNode, childBufPage, childSlotNum)
		return uf, isLeafMerged || lm, err

	// リーフノードの場合: そのまま削除する
	case bytes.Equal(nodeType, node.NodeTypeLeaf):
		leafNode := node.NewLeafNode(pg)
		slotNum, found := leafNode.SearchSlotNum(key)
		if !found {
			return false, false, ErrKeyNotFound
		}
		leafNode.Delete(slotNum)
		return !leafNode.IsHalfFull(), false, nil

	default:
		return false, false, errors.New("unknown node type")
	}
}
