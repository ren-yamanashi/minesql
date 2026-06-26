package btree

import (
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
	height := metaPage.height()
	leafBufPage, err := t.descendToLeafExclusive(mtr, rootPageId, height, record.Key())
	if err != nil {
		return false, err
	}

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
//   - 更新後レコードがリーフに収まらない場合は、対象レコードを削除してから挿入の機構で挿入する
func (t *Tree) updatePessimistic(mtr *buffer.Mtr, record Record) error {
	mtr.LockSharedExclusive(t.latch)
	defer mtr.UnlockLatch(t.latch)

	// メタページを取得
	pageMeta, err := mtr.PageForWrite(t.MetaPageId())
	if err != nil {
		return err
	}
	defer mtr.Unpin(t.MetaPageId())
	metaPage := newMetaPage(pageMeta)

	// リーフまで Exclusive で降下し、対象スロットを特定する
	rootPageId := metaPage.rootPageId()
	height := metaPage.height()
	leafBufPage, err := t.descendToLeafExclusive(mtr, rootPageId, height, record.Key())
	if err != nil {
		return err
	}
	leafNode := newLeafNode(leafBufPage)
	slotNum, found := leafNode.searchSlotNum(record.Key())
	if !found {
		mtr.Unpin(leafBufPage.PageId())
		return ErrKeyNotFound
	}

	// 更新後レコードが最大レコードサイズを超える場合は、削除を行う前にエラーとする
	if len(record.Bytes()) > leafNode.maxRecordSize() {
		mtr.Unpin(leafBufPage.PageId())
		return errRecordTooLarge
	}

	// まずインプレース更新 (リサイズ) を試みる。収まればそれで完了
	if leafNode.update(slotNum, record) {
		mtr.Unpin(leafBufPage.PageId())
		return nil
	}

	// 収まらない場合は対象レコードを削除してから、挿入の機構で挿入する (アンダーフロー処理は行わない)
	leafNode.delete(slotNum)
	mtr.Unpin(leafBufPage.PageId())

	return t.insertWithMetaUpdate(mtr, metaPage, record)
}
