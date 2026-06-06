package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
)

// Delete は B+Tree からレコードを削除する
func (t *Tree) Delete(mtr *buffer.Mtr, key []byte) error {
	needsPessimistic, err := t.deleteOptimistic(mtr, key)
	if err != nil {
		return err
	}
	if !needsPessimistic {
		return nil
	}
	return t.deletePessimistic(mtr, key)
}

// deleteOptimistic は楽観モードで削除を試みる
//   - 削除によりアンダーフローが起こる可能性がある場合は (true, nil) を返し、呼び出し側に悲観モードへの切り替えを促す
func (t *Tree) deleteOptimistic(mtr *buffer.Mtr, key []byte) (needsPessimistic bool, err error) {
	mtr.LockShared(t.latch)
	defer mtr.UnlockLatch(t.latch)

	pageMeta, err := mtr.PageForRead(t.MetaPageId())
	if err != nil {
		return false, err
	}
	defer mtr.Unpin(t.MetaPageId())
	metaPage := newMetaPage(pageMeta)

	rootPageId := metaPage.rootPageId()
	height := metaPage.height()
	leafPageId, err := t.descendToLeafShared(mtr, rootPageId, key)
	if err != nil {
		return false, err
	}

	leafBufPage, err := mtr.PageForWrite(leafPageId)
	if err != nil {
		return false, err
	}
	defer mtr.Unpin(leafPageId)

	leafNode := newLeafNode(leafBufPage)
	slotNum, found := leafNode.searchSlotNum(key)
	if !found {
		return false, ErrKeyNotFound
	}
	// 高さ 1 (ルート = リーフ) ならアンダーフローしても構造変更は起こらないので常に楽観で完了する
	if height >= 2 && !leafNode.canDeleteWithoutUnderflow(slotNum) {
		return true, nil
	}
	leafNode.delete(slotNum)
	return false, nil
}

// deletePessimistic は悲観モードで削除する
func (t *Tree) deletePessimistic(mtr *buffer.Mtr, key []byte) error {
	mtr.LockSharedExclusive(t.latch)
	defer mtr.UnlockLatch(t.latch)

	// メタページを取得
	pageMeta, err := mtr.PageForWrite(t.MetaPageId())
	if err != nil {
		return err
	}
	defer mtr.Unpin(t.MetaPageId())
	metaPage := newMetaPage(pageMeta)

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
		branch := newBranchNode(bufPageRoot)
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
	branchNode := newBranchNode(bufPageRoot)
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
		branchNode := newBranchNode(pg)
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
		leafNode := newLeafNode(pg)
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
