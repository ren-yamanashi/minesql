package fsp

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// FreeSegmentPage は headerAt の segment header が指す segment から id のページを解放する
//   - 解放対象ページの中身には書き込まない
//   - 二重解放、segment に属さないページの解放は panic
func FreeSegmentPage(mtr *buffer.Mtr, headerAt flst.Address, id page.Id) error {
	fileId := id.FileId()
	headerPage, err := mtr.PageForWrite(page.NewId(fileId, 0))
	if err != nil {
		return err
	}
	h := header{bufPage: headerPage}
	entryAddr, err := ReadSegmentHeader(mtr, fileId, headerAt)
	if err != nil {
		return err
	}
	entry, err := loadInodeEntryByAddress(mtr, fileId, entryAddr)
	if err != nil {
		return err
	}
	return freeSegmentPage(mtr, h, entry, id)
}

// freeSegmentPage は entry の segment から id のページを解放する
func freeSegmentPage(mtr *buffer.Mtr, h header, entry inodeEntry, id page.Id) error {
	fileId := id.FileId()
	x, err := loadEntryByPageNumber(mtr, fileId, h, id.PageNumber())
	if err != nil {
		return err
	}
	pos := int(id.PageNumber() % extentPageCount)
	if x.isPageFree(pos) {
		panic(fmt.Sprintf("fsp: double free of page (pageNumber=%d)", id.PageNumber()))
	}
	state := x.state()
	switch state {
	case stateFreeFrag, stateFullFrag:
		return freeSegmentFragPage(mtr, entry, id)
	case stateFseg, stateFsegFrag:
		return freeSegmentExtentPage(mtr, fileId, h, entry, x, pos)
	default:
		panic(fmt.Sprintf("fsp: cannot free segment page in extent state %s (pageNumber=%d)", state, id.PageNumber()))
	}
}

// freeSegmentFragPage は frag array に登録されたページを解放する
//   - id が frag array にない場合は panic (この segment に属さないページ)
//   - slot 無効化の前に FreePage の touch フェーズを呼ぶことで、書き込み開始後の新規ページ取得を避ける
func freeSegmentFragPage(mtr *buffer.Mtr, entry inodeEntry, id page.Id) error {
	slot := -1
	for i := range inodeFragSlotCount {
		if entry.fragSlot(i) == id.PageNumber() {
			slot = i
			break
		}
	}
	if slot < 0 {
		panic(fmt.Sprintf("fsp: page %d is not registered in segment frag array", id.PageNumber()))
	}
	plan, err := touchFreePage(mtr, id)
	if err != nil {
		return err
	}
	entry.setFragSlot(slot, page.MaxPageNumber)
	writeFreePage(mtr, id, plan)
	return nil
}

// freeSegmentExtentPage は専有 extent 内のページを 1 枚解放する
func freeSegmentExtentPage(mtr *buffer.Mtr, fileId page.FileId, h header, entry inodeEntry, x xdesEntry, pos int) error {
	if x.segmentId() != entry.segId() {
		panic(fmt.Sprintf("fsp: extent segment id %d does not match inode segment id %d", x.segmentId(), entry.segId()))
	}
	wasFull := x.isAllUsed()
	freeAfter := x.freePageCount() + 1
	returnable := isReturnableAfterFree(x.state(), freeAfter)
	if wasFull {
		if err := touchTransitionPages(mtr, fileId, x, entry.notFullListBase()); err != nil {
			return err
		}
	}
	if returnable {
		destBase := spaceReturnBase(h, x.state())
		if err := touchTransitionPages(mtr, fileId, x, destBase); err != nil {
			return err
		}
	}
	if wasFull {
		nodeAddr := x.flstNodeAddress()
		if err := flst.Remove(mtr, fileId, entry.fullListBase(), nodeAddr); err != nil {
			panicOnPostWriteFlstErr(err)
		}
		if err := flst.AddLast(mtr, fileId, entry.notFullListBase(), nodeAddr); err != nil {
			panicOnPostWriteFlstErr(err)
		}
		entry.setNotFullNUsed(entry.notFullNUsed() + uint32(extentPageCount-1))
	} else {
		if entry.notFullNUsed() == 0 {
			panic("fsp: NOT_FULL_N_USED underflow on segment page free")
		}
		entry.setNotFullNUsed(entry.notFullNUsed() - 1)
	}
	x.setPageFree(pos, true)
	if !returnable {
		return nil
	}
	if x.state() == stateFsegFrag {
		if entry.notFullNUsed() == 0 {
			panic("fsp: NOT_FULL_N_USED underflow on lease extent return")
		}
		entry.setNotFullNUsed(entry.notFullNUsed() - 1)
	}
	if err := flst.Remove(mtr, fileId, entry.notFullListBase(), x.flstNodeAddress()); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	freeExtentToSpace(mtr, fileId, h, x)
	return nil
}

// isReturnableAfterFree は 1 ページ解放後に extent を空間へ返却するかを返す
func isReturnableAfterFree(state extentState, freeAfter int) bool {
	switch state {
	case stateFseg:
		return freeAfter == extentPageCount
	case stateFsegFrag:
		return freeAfter == extentPageCount-1
	default:
		return false
	}
}

// spaceReturnBase は state に応じた空間側リストの base アドレスを返す
func spaceReturnBase(h header, state extentState) flst.Address {
	if state == stateFseg {
		return h.freeListBase()
	}
	return h.freeFragListBase()
}
