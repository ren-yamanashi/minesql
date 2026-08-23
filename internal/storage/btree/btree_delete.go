package btree

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
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
	leafBufPage, err := t.descendToLeafExclusive(mtr, rootPageId, height, key)
	if err != nil {
		return false, err
	}

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
	metaPage := newMetaPage(pageMeta)

	// ルートページを取得
	rootPageId := metaPage.rootPageId()
	bufPageRoot, err := mtr.PageForRead(rootPageId)
	if err != nil {
		return err
	}

	// 再帰的に削除
	_, err = t.deleteRecursively(mtr, bufPageRoot, key, metaPage)
	if err != nil {
		return err
	}

	// ルートノードがブランチノードでレコード数が 0 になった場合、子をルートにする
	if nodeType(bufPageRoot.Data()) != nodeTypeBranch {
		return nil
	}
	branch := newBranchNode(bufPageRoot)
	if branch.numRecords() != 0 {
		return nil
	}

	// 縮退確定後に旧ルートの解放を事前取得する。取得に失敗した場合は縮退を見送り、次回の削除で再試行する
	plan, err := fsp.TouchFreeSegmentPage(mtr, t.branchSegmentHeaderAt(), rootPageId)
	if err != nil {
		return nil
	}
	newRootPageId := branch.rightChildPageId()
	metaPage.setRootPageId(newRootPageId)
	metaPage.setHeight(metaPage.height() - 1)
	fsp.WriteFreeSegmentPage(mtr, plan)
	return nil
}

// deleteRecursively は再帰的にノードを辿ってレコードを削除する
//   - bufPage: 削除先のノードのバッファページ
//   - key: 削除するキー
//   - metaPage: リーフマージ時に leafPageCount を減算するために引き渡す
//   - return: underflow (アンダーフローが発生したか)
func (t *Tree) deleteRecursively(mtr *buffer.Mtr, bufPage *buffer.Page, key []byte, metaPage *metaPage) (underflow bool, err error) {
	pg, err := mtr.PageForWrite(bufPage.PageId())
	if err != nil {
		return false, err
	}
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
			return false, err
		}
		childBufPage, err := mtr.PageForRead(childPageId)
		if err != nil {
			return false, err
		}
		defer mtr.Unpin(childPageId)

		// 子ノードに対して削除処理を再帰的に実行
		underflow, err := t.deleteRecursively(mtr, childBufPage, key, metaPage)
		if err != nil {
			return false, err
		}
		// 子ノードがアンダーフローしなかった場合、終了
		if !underflow {
			return false, nil
		}
		// 子ノードがアンダーフローした場合、兄弟ノードとマージ
		return t.deleteUnderflow(mtr, branchNode, childBufPage, childSlotNum, metaPage)

	// リーフノードの場合: そのまま削除する
	case nodeTypeLeaf:
		leafNode := newLeafNode(pg)
		slotNum, found := leafNode.searchSlotNum(key)
		if !found {
			return false, ErrKeyNotFound
		}
		leafNode.delete(slotNum)
		return !leafNode.isHalfFull(), nil

	default:
		panic(fmt.Sprintf("btree: unknown node type %q at pageId=%v", nt, bufPage.PageId()))
	}
}
