package btree

import (
	"bytes"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// Iterator は B+Tree のリーフノードを走査する
//   - リーフページの Pin と Shared ラッチは Mtr が保持し、Mtr の完了時に一括解放される
//   - 直前に読んだキーがあればそのキーで、初回 Get 前であれば走査開始時の SearchMode で位置を取り直す
type Iterator struct {
	tree            *Tree
	mtr             *buffer.Mtr
	bufferPage      *buffer.Page
	slotNum         int
	searchMode      SearchMode
	lastKey         []byte
	modifyCountSnap uint64
}

// NewIterator は Tree.Search で得たリーフページ・スロット位置・降下時の SearchMode を引き継いだ Iterator を返す
//   - searchMode: 初回 Get 前にページが変更された場合、ここに渡したモードで再降下して位置を取り直す
//   - mtr: リーフページの Pin と Shared ラッチを保持し続ける Mtr。 Iterator の生存期間中は同じ Mtr を使う
func NewIterator(tree *Tree, mtr *buffer.Mtr, bufPage *buffer.Page, slotNum int, searchMode SearchMode) *Iterator {
	return &Iterator{
		tree:            tree,
		mtr:             mtr,
		bufferPage:      bufPage,
		slotNum:         slotNum,
		searchMode:      searchMode,
		modifyCountSnap: bufPage.ModifyCount(),
	}
}

// Mtr は Iterator が保持する mini-transaction を返す
func (it *Iterator) Mtr() *buffer.Mtr {
	return it.mtr
}

// BufferPageId は現在 Iterator が参照しているリーフページの ID を返す
//   - 呼び出し側が Pin を明示解放するときに mtr.Unpin の引数として使う
func (it *Iterator) BufferPageId() page.Id {
	return it.bufferPage.PageId()
}

// Get は現在参照しているリーフノードのレコードを取得する
func (it *Iterator) Get() (Record, bool, error) {
	if _, err := it.checkAndRefetch(); err != nil {
		return NewRecord(nil, nil, nil), false, err
	}

	leaf := newLeafNode(it.bufferPage)
	if it.slotNum >= leaf.numRecords() {
		return NewRecord(nil, nil, nil), false, nil
	}
	record := leaf.record(it.slotNum)
	header := bytes.Clone(record.Header())
	key := bytes.Clone(record.Key())
	nonKey := bytes.Clone(record.NonKey())

	it.lastKey = key
	it.modifyCountSnap = it.bufferPage.ModifyCount()
	return NewRecord(header, key, nonKey), true, nil
}

// Next は次のレコードを取得する
func (it *Iterator) Next() (Record, bool, error) {
	record, ok, err := it.Get()
	if err != nil {
		return NewRecord(nil, nil, nil), false, err
	}
	if !ok {
		return NewRecord(nil, nil, nil), false, nil
	}
	if err := it.Advance(); err != nil {
		return NewRecord(nil, nil, nil), false, err
	}
	return record, true, nil
}

// Advance は次のレコードに進む
//   - refetch が起きた場合は refetchByKey が既に「次の未読位置」へ位置付けているため、追加の slotNum++ は行わない
func (it *Iterator) Advance() error {
	refetched, err := it.checkAndRefetch()
	if err != nil {
		return err
	}
	if refetched {
		return nil
	}

	leaf := newLeafNode(it.bufferPage)
	if it.slotNum < leaf.numRecords() {
		it.slotNum++
	}
	if it.slotNum < leaf.numRecords() {
		it.modifyCountSnap = it.bufferPage.ModifyCount()
		return nil
	}
	nextPageId := leaf.nextPageId()
	if nextPageId.IsInvalid() {
		return nil
	}

	// ラッチカップリング: 手元のリーフの S を保持したまま次リーフを Mtr 経由で取得し、
	// 取得後に手元のリーフを Mtr.Unpin で解放する (取得順序は左 → 右)
	nextPage, err := it.mtr.PageForRead(nextPageId)
	if err != nil {
		return err
	}
	oldPageId := it.bufferPage.PageId()
	it.mtr.Unpin(oldPageId)

	it.bufferPage = nextPage
	it.slotNum = 0
	it.modifyCountSnap = nextPage.ModifyCount()
	return nil
}

// checkAndRefetch は更新カウンタを照合し、変化があれば直前に読んだキーで位置を取り直す
//   - 戻り値 refetched: refetchByKey を実行した場合 true。呼び出し側はこれを見て二重インクリメントを避ける
func (it *Iterator) checkAndRefetch() (refetched bool, err error) {
	current := it.bufferPage.ModifyCount()
	if current == it.modifyCountSnap {
		return false, nil
	}
	if it.lastKey == nil {
		// まだ 1 件も読んでいないので、走査開始時の SearchMode で再降下して位置を取り直す
		if err := it.refetchBySearchMode(); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := it.refetchByKey(it.lastKey); err != nil {
		return false, err
	}
	return true, nil
}

// refetchBySearchMode は走査開始時の SearchMode で Tree.Search を再実行し、リーフと開始スロットを取り直す
//   - lastKey == nil (= 初回 Get 前) からの再位置付けに使う
func (it *Iterator) refetchBySearchMode() error {
	oldPageId := it.bufferPage.PageId()
	iter, err := it.tree.Search(it.mtr, it.searchMode)
	if err != nil {
		return err
	}

	// 古いリーフの Pin を解放し、 Tree.Search が確保した新リーフに切り替える
	it.mtr.Unpin(oldPageId)
	it.bufferPage = iter.bufferPage
	it.slotNum = iter.slotNum
	it.modifyCountSnap = iter.bufferPage.ModifyCount()
	return nil
}

// refetchByKey は指定キーで Tree.Search を再実行し、リーフと「次の未読位置」を取り直す
//   - lastKey が削除されている場合は新しい位置のレコードがそのまま「次の未読」となる
//   - lastKey が残っている場合は既読位置の次のスロットへ進める
func (it *Iterator) refetchByKey(key []byte) error {
	oldPageId := it.bufferPage.PageId()
	iter, err := it.tree.Search(it.mtr, SearchModeKey{Key: key})
	if err != nil {
		return err
	}

	newLeaf := newLeafNode(iter.bufferPage)
	var newSlot int
	if iter.slotNum < newLeaf.numRecords() && bytes.Equal(newLeaf.record(iter.slotNum).Key(), key) {
		newSlot = iter.slotNum + 1
	} else {
		newSlot = iter.slotNum
	}

	// 古いリーフの Pin を解放し、 Tree.Search が確保した新リーフに切り替える
	it.mtr.Unpin(oldPageId)
	it.bufferPage = iter.bufferPage
	it.slotNum = newSlot
	it.modifyCountSnap = iter.bufferPage.ModifyCount()
	return nil
}
