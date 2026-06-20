package btree

import (
	"bytes"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
)

// Iterator は B+Tree のリーフノードを走査する
//   - 走査の生存期間中はバッファページの Pin を保持する
//   - フェッチごとに短期の Shared ラッチを取り、解放後の更新を更新カウンタで検知する
//   - 直前に読んだキーがあればそのキーで、初回 Get 前であれば走査開始時の SearchMode で位置を取り直す
type Iterator struct {
	tree            *Tree
	bufferPage      *buffer.Page
	slotNum         int
	searchMode      SearchMode
	lastKey         []byte
	modifyCountSnap uint64
}

// NewIterator は Tree.Search で得たリーフページ・スロット位置・降下時の SearchMode を引き継いだ Iterator を返す
//   - searchMode: 初回 Get 前にページが変更された場合、ここに渡したモードで再降下して位置を取り直す
func NewIterator(tree *Tree, bufPage *buffer.Page, slotNum int, searchMode SearchMode) *Iterator {
	return &Iterator{
		tree:            tree,
		bufferPage:      bufPage,
		slotNum:         slotNum,
		searchMode:      searchMode,
		modifyCountSnap: bufPage.ModifyCount(),
	}
}

// Close はイテレータが保持しているバッファページの Pin を解放する
func (it *Iterator) Close() {
	it.tree.bufferPool.Unpin(it.bufferPage.PageId())
}

// Get は現在参照しているリーフノードのレコードを取得する
func (it *Iterator) Get() (Record, bool, error) {
	if _, err := it.checkAndRefetch(); err != nil {
		return NewRecord(nil, nil, nil), false, err
	}

	it.bufferPage.Latch().LockShared()
	defer it.bufferPage.Latch().Unlock(buffer.LatchShared)

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

	it.bufferPage.Latch().LockShared()
	leaf := newLeafNode(it.bufferPage)
	if it.slotNum < leaf.numRecords() {
		it.slotNum++
	}
	if it.slotNum < leaf.numRecords() {
		it.modifyCountSnap = it.bufferPage.ModifyCount()
		it.bufferPage.Latch().Unlock(buffer.LatchShared)
		return nil
	}
	nextPageId := leaf.nextPageId()

	if nextPageId.IsInvalid() {
		it.bufferPage.Latch().Unlock(buffer.LatchShared)
		return nil
	}

	// ラッチカップリング: 手元のリーフの S を保持したまま次リーフを Pin して S を取り、
	// 更新カウンタを記録してから手元のリーフを解放する (取得順序は左 → 右)
	nextPage, err := it.tree.bufferPool.Page(nextPageId)
	if err != nil {
		it.bufferPage.Latch().Unlock(buffer.LatchShared)
		return err
	}
	nextPage.Latch().LockShared()
	nextModifyCount := nextPage.ModifyCount()

	oldPageId := it.bufferPage.PageId()
	it.bufferPage.Latch().Unlock(buffer.LatchShared)
	it.tree.bufferPool.Unpin(oldPageId)
	nextPage.Latch().Unlock(buffer.LatchShared)

	it.bufferPage = nextPage
	it.slotNum = 0
	it.modifyCountSnap = nextModifyCount
	return nil
}

// checkAndRefetch は更新カウンタを照合し、変化があれば直前に読んだキーで位置を取り直す
//   - 戻り値 refetched: refetchByKey を実行した場合 true。呼び出し側はこれを見て二重インクリメントを避ける
func (it *Iterator) checkAndRefetch() (refetched bool, err error) {
	it.bufferPage.Latch().LockShared()
	current := it.bufferPage.ModifyCount()
	it.bufferPage.Latch().Unlock(buffer.LatchShared)

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
	mtr := buffer.NewMtr(it.tree.bufferPool)
	defer mtr.UnpinAll()

	iter, err := it.tree.Search(mtr, it.searchMode)
	if err != nil {
		return err
	}

	iter.bufferPage.Latch().LockShared()
	newSlot := iter.slotNum
	newModifyCount := iter.bufferPage.ModifyCount()
	iter.bufferPage.Latch().Unlock(buffer.LatchShared)

	// 既存の Pin を解放し、Tree.Search が確保した新リーフの Pin を引き継ぐ
	it.tree.bufferPool.Unpin(it.bufferPage.PageId())
	it.bufferPage = iter.bufferPage
	it.slotNum = newSlot
	it.modifyCountSnap = newModifyCount
	return nil
}

// refetchByKey は指定キーで Tree.Search を再実行し、リーフと「次の未読位置」を取り直す
//   - lastKey が削除されている場合は新しい位置のレコードがそのまま「次の未読」となる
//   - lastKey が残っている場合は既読位置の次のスロットへ進める
func (it *Iterator) refetchByKey(key []byte) error {
	mtr := buffer.NewMtr(it.tree.bufferPool)
	defer mtr.UnpinAll()

	iter, err := it.tree.Search(mtr, SearchModeKey{Key: key})
	if err != nil {
		return err
	}

	iter.bufferPage.Latch().LockShared()
	newLeaf := newLeafNode(iter.bufferPage)
	var newSlot int
	if iter.slotNum < newLeaf.numRecords() && bytes.Equal(newLeaf.record(iter.slotNum).Key(), key) {
		newSlot = iter.slotNum + 1
	} else {
		newSlot = iter.slotNum
	}
	newModifyCount := iter.bufferPage.ModifyCount()
	iter.bufferPage.Latch().Unlock(buffer.LatchShared)

	// 既存の Pin を解放し、Tree.Search が確保した新リーフの Pin を引き継ぐ
	it.tree.bufferPool.Unpin(it.bufferPage.PageId())
	it.bufferPage = iter.bufferPage
	it.slotNum = newSlot
	it.modifyCountSnap = newModifyCount
	return nil
}
