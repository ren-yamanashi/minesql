package btree

import (
	"bytes"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// ScanIterator は現在リーフの Pin のみを保持して B+Tree を走査するイテレータ
//   - ラッチは各レコード取得の mini-transaction でのみ取り、取得後に解放される
//   - 走査終端に達すると保持中の Pin は自動解放される。途中で走査を打ち切る場合は Close を呼ぶ
type ScanIterator struct {
	tree            *Tree
	pool            *buffer.Pool
	pinnedPageId    page.Id
	slotNum         int
	searchMode      SearchMode
	lastKey         []byte
	modifyCountSnap uint64
	done            bool
}

// OpenScan は走査イテレータを返す
//   - 降下は内部の mini-transaction で行い、リーフ到達時にラッチを解放して Pin のみをイテレータへ移譲する
func (t *Tree) OpenScan(mode SearchMode) (*ScanIterator, error) {
	mtr := buffer.NewMtr(t.bufferPool)
	iter, err := t.Search(mtr, mode)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	leafPageId := iter.bufferPage.PageId()
	modifyCountSnap := iter.bufferPage.ModifyCount()
	slotNum := iter.slotNum

	mtr.TransferPin(leafPageId)
	mtr.UnpinAll()

	return &ScanIterator{
		tree:            t,
		pool:            t.bufferPool,
		pinnedPageId:    leafPageId,
		slotNum:         slotNum,
		searchMode:      mode,
		modifyCountSnap: modifyCountSnap,
	}, nil
}

// Next は次のレコードと、そのレコード取得で開いた mini-transaction を返す
//   - 返る mtr は現在リーフの S ラッチを保持している。呼び出し側は同じ mtr で追加のページ参照を
//     行ってよく、使い終えたら UnpinAll で閉じる
//   - ok=false (走査終端) のとき mtr は nil
//   - エラーを返した後のイテレータは再利用できない。Close を呼んで破棄する
func (it *ScanIterator) Next() (Record, *buffer.Mtr, bool, error) {
	if it.done {
		return NewRecord(nil, nil, nil), nil, false, nil
	}

	mtr := buffer.NewMtr(it.pool)
	bufPage, err := mtr.PageForRead(it.pinnedPageId)
	if err != nil {
		mtr.UnpinAll()
		return NewRecord(nil, nil, nil), nil, false, err
	}

	bufPage, err = it.recoverPositionIfChanged(mtr, bufPage)
	if err != nil {
		mtr.UnpinAll()
		return NewRecord(nil, nil, nil), nil, false, err
	}

	record, ok := it.readCurrent(bufPage)
	if !ok {
		it.releasePin()
		mtr.UnpinAll()
		return NewRecord(nil, nil, nil), nil, false, nil
	}

	if err := it.advance(mtr, bufPage); err != nil {
		mtr.UnpinAll()
		return NewRecord(nil, nil, nil), nil, false, err
	}
	return record, mtr, true, nil
}

// Close は保持中のリーフ Pin を解放する (終端到達後・Close 済みの呼び出しは無害)
func (it *ScanIterator) Close() {
	it.releasePin()
}

// recoverPositionIfChanged は更新カウンタの変化を検知したときに直前キー (初回は searchMode) で位置を取り直す
//   - 再降下の前に旧リーフの S ラッチと mtr 側 Pin を解放し、ルートからのラッチ取得順序を守る
//   - リーフが変わった場合は新リーフの S ラッチを mtr に残したまま、走査保持 Pin だけを新リーフへ付け替える
//   - 取り直し後の位置がリーフ末端に達している場合は次リーフへ順に進む
//   - 戻り値は現在参照すべきリーフの bufPage (変化なしなら引数と同じ、切り替え時は新リーフ)
func (it *ScanIterator) recoverPositionIfChanged(mtr *buffer.Mtr, bufPage *buffer.Page) (*buffer.Page, error) {
	if bufPage.ModifyCount() == it.modifyCountSnap {
		return bufPage, nil
	}

	mode := SearchMode(SearchModeKey{Key: it.lastKey})
	useLastKey := it.lastKey != nil
	if !useLastKey {
		mode = it.searchMode
	}

	// 再降下前に旧リーフの S ラッチと mtr 側 Pin を解放する (走査保持 Pin は pool 側に残る)
	mtr.Unpin(it.pinnedPageId)

	newIter, err := it.tree.Search(mtr, mode)
	if err != nil {
		return nil, err
	}

	curPage := newIter.bufferPage
	curPageId := curPage.PageId()
	newSlot := newIter.slotNum
	if useLastKey {
		curLeaf := newLeafNode(curPage)
		if newSlot < curLeaf.numRecords() && bytes.Equal(curLeaf.record(newSlot).Key(), it.lastKey) {
			newSlot++
		}
	}

	for {
		leaf := newLeafNode(curPage)
		if newSlot < leaf.numRecords() {
			break
		}
		nextPageId := leaf.nextPageId()
		if nextPageId.IsInvalid() {
			break
		}
		nextPage, err := mtr.PageForRead(nextPageId)
		if err != nil {
			return nil, err
		}
		curPage = nextPage
		curPageId = nextPageId
		newSlot = 0
	}

	if curPageId != it.pinnedPageId {
		if _, err := it.pool.Page(curPageId); err != nil {
			return nil, err
		}
		it.pool.Unpin(it.pinnedPageId)
		it.pinnedPageId = curPageId
	}
	it.slotNum = newSlot
	it.modifyCountSnap = curPage.ModifyCount()
	return curPage, nil
}

// readCurrent は現在スロットのレコードを読みクローンして返し、lastKey / modifyCountSnap を更新する
func (it *ScanIterator) readCurrent(bufPage *buffer.Page) (Record, bool) {
	leaf := newLeafNode(bufPage)
	if it.slotNum >= leaf.numRecords() {
		return NewRecord(nil, nil, nil), false
	}
	record := leaf.record(it.slotNum)
	header := bytes.Clone(record.Header())
	key := bytes.Clone(record.Key())
	nonKey := bytes.Clone(record.NonKey())

	it.lastKey = key
	it.modifyCountSnap = bufPage.ModifyCount()
	return NewRecord(header, key, nonKey), true
}

// advance は次のレコードへ位置を進める
//   - 同一リーフ内なら slotNum++
//   - リーフ末端に達したら次リーフの S ラッチを同 mtr で取り、空リーフの間は更に次リーフへ送り越す
//   - 最終到達リーフのカウンタを記録してから、走査保持 Pin を新リーフへ付け替える
func (it *ScanIterator) advance(mtr *buffer.Mtr, bufPage *buffer.Page) error {
	leaf := newLeafNode(bufPage)
	it.slotNum++
	if it.slotNum < leaf.numRecords() {
		it.modifyCountSnap = bufPage.ModifyCount()
		return nil
	}
	nextPageId := leaf.nextPageId()
	if nextPageId.IsInvalid() {
		return nil
	}

	curPage, err := mtr.PageForRead(nextPageId)
	if err != nil {
		return err
	}
	curPageId := nextPageId
	for newLeafNode(curPage).numRecords() == 0 {
		nextPageId := newLeafNode(curPage).nextPageId()
		if nextPageId.IsInvalid() {
			break
		}
		nextPage, err := mtr.PageForRead(nextPageId)
		if err != nil {
			return err
		}
		curPage = nextPage
		curPageId = nextPageId
	}
	nextModifyCount := curPage.ModifyCount()

	// 最終到達リーフの S ラッチとカウンタを取ってから、走査保持用の Pin を付け替える
	mtr.TransferPin(curPageId)
	it.pool.Unpin(it.pinnedPageId)
	it.pinnedPageId = curPageId
	it.slotNum = 0
	it.modifyCountSnap = nextModifyCount
	return nil
}

// releasePin は保持中のリーフ Pin を Pool.Unpin で解放し、done を立てる
func (it *ScanIterator) releasePin() {
	if it.done {
		return
	}
	it.pool.Unpin(it.pinnedPageId)
	it.done = true
}
