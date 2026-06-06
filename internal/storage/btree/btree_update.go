package btree

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
)

// Update は B+Tree の特定のノードの値を更新する
func (t *Tree) Update(mtr *buffer.Mtr, record Record) error {
	needsPessimistic, err := t.updateOptimistic(mtr, record)
	if err != nil {
		return err
	}
	if !needsPessimistic {
		return nil
	}
	return t.updatePessimistic(mtr, record)
}

// updateOptimistic は楽観モードで更新を試みる
//   - サイズ増加でリーフに収まらない場合は (true, nil) を返し、呼び出し側に悲観モードへの切り替えを促す
func (t *Tree) updateOptimistic(mtr *buffer.Mtr, record Record) (needsPessimistic bool, err error) {
	mtr.LockShared(t.latch)
	defer mtr.UnlockLatch(t.latch)

	pageMeta, err := mtr.PageForRead(t.MetaPageId())
	if err != nil {
		return false, err
	}
	defer mtr.Unpin(t.MetaPageId())
	metaPage := newMetaPage(pageMeta)

	rootPageId := metaPage.rootPageId()
	leafPageId, err := t.descendToLeafShared(mtr, rootPageId, record.Key())
	if err != nil {
		return false, err
	}

	leafBufPage, err := mtr.PageForWrite(leafPageId)
	if err != nil {
		return false, err
	}
	defer mtr.Unpin(leafPageId)

	leafNode := newLeafNode(leafBufPage)
	slotNum, found := leafNode.searchSlotNum(record.Key())
	if !found {
		return false, ErrKeyNotFound
	}
	if !leafNode.canFitUpdate(slotNum, record) {
		return true, nil
	}
	leafNode.update(slotNum, record)
	return false, nil
}

// updatePessimistic は悲観モードで更新する
func (t *Tree) updatePessimistic(mtr *buffer.Mtr, record Record) error {
	mtr.LockSharedExclusive(t.latch)
	defer mtr.UnlockLatch(t.latch)

	// メタページを取得
	pageMeta, err := mtr.PageForRead(t.MetaPageId())
	if err != nil {
		return err
	}
	metaPage := newMetaPage(pageMeta)
	defer mtr.Unpin(t.MetaPageId())

	// ルートページ取得
	rootPageId := metaPage.rootPageId()
	rootBufPage, err := mtr.PageForRead(rootPageId)
	if err != nil {
		return err
	}
	defer mtr.Unpin(rootPageId)
	return t.updateRecursively(mtr, rootBufPage, record)
}

// updateRecursively は再帰的にノードを辿ってレコードを更新する
func (t *Tree) updateRecursively(mtr *buffer.Mtr, bufPage *buffer.Page, record Record) error {
	pg, err := mtr.PageForWrite(bufPage.PageId())
	if err != nil {
		return err
	}
	defer mtr.Unpin(bufPage.PageId())

	nt := nodeType(pg.Data())
	switch nt {
	// ブランチノードの場合: 子ノードに対して再帰実行する
	case nodeTypeBranch:
		branchNode := newBranchNode(pg)
		mode := SearchModeKey{Key: record.Key()}
		childPageId, err := mode.childPageId(branchNode)
		if err != nil {
			return err
		}
		childBufPage, err := mtr.PageForRead(childPageId)
		if err != nil {
			return err
		}
		defer mtr.Unpin(childPageId)
		return t.updateRecursively(mtr, childBufPage, record)

	// リーフノードの場合: そのまま更新する
	case nodeTypeLeaf:
		leafNode := newLeafNode(pg)
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
