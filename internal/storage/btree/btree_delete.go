package btree

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
)

// Delete は B+Tree からレコードを削除する
func (t *Tree) Delete(key []byte) error {
	// メタページを取得
	pageMeta, err := t.bufferPool.PageForWrite(t.MetaPageId())
	if err != nil {
		return err
	}
	defer t.bufferPool.UnRefPage(t.MetaPageId())
	metaPage := newMetaPage(pageMeta.Page)

	// ルートページを取得
	rootPageId := metaPage.rootPageId()
	bufPageRoot, err := t.bufferPool.PageForRead(rootPageId)
	if err != nil {
		return err
	}

	// 再帰的に削除
	underflow, isLeafMerged, err := t.deleteRecursively(bufPageRoot, key)
	if err != nil {
		return err
	}

	// ルートノードがブランチノードで、子が 1 つになった場合 (=ブランチノード1, リーフノード1 になった場合)、子をルートにする
	var isRootCollapsed bool
	pageRoot, err := t.bufferPool.PageForRead(bufPageRoot.PageId)
	if err != nil {
		return err
	}
	defer t.bufferPool.UnRefPage(bufPageRoot.PageId)
	if underflow && bytes.Equal(nodeType(pageRoot.Page), nodeTypeBranch) {
		branch := newBranchNode(pageRoot.Page)
		if branch.numRecords() == 0 {
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
	branchNode := newBranchNode(pageRoot.Page)
	newRootPageId := branchNode.rightChildPageId()
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
func (t *Tree) deleteRecursively(bufPage *buffer.Page, key []byte) (underflow bool, isLeafMerged bool, err error) {
	pg, err := t.bufferPool.PageForWrite(bufPage.PageId)
	if err != nil {
		return false, false, err
	}
	nt := nodeType(pg.Page)

	switch {
	// ブランチノードの場合: 子ノードに対して再帰実行する
	case bytes.Equal(nt, nodeTypeBranch):
		// 削除先の子ノードを取得
		branchNode := newBranchNode(pg.Page)
		childSlotNum, found := branchNode.searchSlotNum(key)
		if found {
			childSlotNum++ // 境界キーと一致する場合、右の子に属する
		}
		childPageId, err := branchNode.childPageId(childSlotNum)
		if err != nil {
			return false, false, err
		}
		childBufPage, err := t.bufferPool.PageForRead(childPageId)
		if err != nil {
			return false, false, err
		}
		defer t.bufferPool.UnRefPage(childPageId)

		// 子ノードに対して削除処理を再帰的に実行
		underflow, isLeafMerged, err := t.deleteRecursively(childBufPage, key)
		if err != nil {
			return false, false, err
		}
		// 子ノードがアンダーフローしなかった場合、終了
		if !underflow {
			return false, isLeafMerged, nil
		}
		// 子ノードがアンダーフローした場合、兄弟ノードとマージ
		uf, lm, err := t.deleteUnderflow(branchNode, childBufPage, childSlotNum)
		return uf, isLeafMerged || lm, err

	// リーフノードの場合: そのまま削除する
	case bytes.Equal(nt, nodeTypeLeaf):
		leafNode := newLeafNode(pg.Page)
		slotNum, found := leafNode.searchSlotNum(key)
		if !found {
			return false, false, ErrKeyNotFound
		}
		leafNode.delete(slotNum)
		return !leafNode.isHalfFull(), false, nil

	default:
		return false, false, errors.New("unknown node type")
	}
}
