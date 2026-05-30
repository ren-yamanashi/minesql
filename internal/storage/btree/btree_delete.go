package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
)

// Delete は B+Tree からレコードを削除する
func (t *Tree) Delete(mtr *buffer.Mtr, key []byte) error {
	// メタページを取得
	pageMeta, err := mtr.PageForWrite(t.MetaPageId())
	if err != nil {
		return err
	}
	defer mtr.Unpin(t.MetaPageId())
	metaPage := newMetaPage(pageMeta.Data())

	// ルートページを取得
	rootPageId := metaPage.rootPageId()
	bufPageRoot, err := mtr.PageForRead(rootPageId)
	if err != nil {
		return err
	}
	defer mtr.Unpin(rootPageId)

	// 再帰的に削除
	underflow, isLeafMerged, err := t.deleteRecursively(mtr, bufPageRoot, key)
	if err != nil {
		return err
	}

	// ルートノードがブランチノードで、子が 1 つになった場合 (=ブランチノード1, リーフノード1 になった場合)、子をルートにする
	var isRootCollapsed bool
	if underflow && nodeType(bufPageRoot.Data()) == nodeTypeBranch {
		branch := newBranchNode(bufPageRoot.Data())
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
	branchNode := newBranchNode(bufPageRoot.Data())
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
func (t *Tree) deleteRecursively(mtr *buffer.Mtr, bufPage *buffer.Page, key []byte) (underflow bool, isLeafMerged bool, err error) {
	pg, err := mtr.PageForWrite(bufPage.PageId())
	if err != nil {
		return false, false, err
	}
	defer mtr.Unpin(bufPage.PageId())
	nt := nodeType(pg.Data())

	switch nt {
	// ブランチノードの場合: 子ノードに対して再帰実行する
	case nodeTypeBranch:
		// 削除先の子ノードを取得
		branchNode := newBranchNode(pg.Data())
		childSlotNum, found := branchNode.searchSlotNum(key)
		if found {
			childSlotNum++ // 境界キーと一致する場合、右の子に属する
		}
		childPageId, err := branchNode.childPageId(childSlotNum)
		if err != nil {
			return false, false, err
		}
		childBufPage, err := mtr.PageForRead(childPageId)
		if err != nil {
			return false, false, err
		}
		defer mtr.Unpin(childPageId)

		// 子ノードに対して削除処理を再帰的に実行
		underflow, isLeafMerged, err := t.deleteRecursively(mtr, childBufPage, key)
		if err != nil {
			return false, false, err
		}
		// 子ノードがアンダーフローしなかった場合、終了
		if !underflow {
			return false, isLeafMerged, nil
		}
		// 子ノードがアンダーフローした場合、兄弟ノードとマージ
		uf, lm, err := t.deleteUnderflow(mtr, branchNode, childBufPage, childSlotNum)
		return uf, isLeafMerged || lm, err

	// リーフノードの場合: そのまま削除する
	case nodeTypeLeaf:
		leafNode := newLeafNode(pg.Data())
		slotNum, found := leafNode.searchSlotNum(key)
		if !found {
			return false, false, ErrKeyNotFound
		}
		leafNode.delete(slotNum)
		return !leafNode.isHalfFull(), false, nil

	default:
		return false, false, errUnknownNodeType
	}
}
