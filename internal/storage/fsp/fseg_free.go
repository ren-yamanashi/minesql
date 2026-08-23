package fsp

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// SegmentPageFreePlan は segment ページ解放の書き込みフェーズが必要とする事前情報
//   - TouchFreeSegmentPage が返し、WriteFreeSegmentPage が消費する
type SegmentPageFreePlan struct {
	h          header
	entry      inodeEntry
	id         page.Id
	isFrag     bool
	fragSlot   int
	fragPlan   freePagePlan
	xdes       xdesEntry
	pos        int
	wasFull    bool
	returnable bool
}

// FreeSegmentPage は headerAt の segment header が指す segment から id のページを解放する
//   - 解放対象ページの中身には書き込まない
//   - 二重解放、segment に属さないページの解放は panic
//   - TouchFreeSegmentPage と WriteFreeSegmentPage の合成
func FreeSegmentPage(mtr *buffer.Mtr, headerAt flst.Address, id page.Id) error {
	plan, err := TouchFreeSegmentPage(mtr, headerAt, id)
	if err != nil {
		return err
	}
	WriteFreeSegmentPage(mtr, plan)
	return nil
}

// TouchFreeSegmentPage は segment ページ解放の事前取得フェーズを実行し、書き込みフェーズの計画を返す
//   - 書き込みフェーズで必要になる全ページを取得して pin する
//   - 二重解放、segment に属さないページの解放は panic
//   - 呼び出し後は書き込み前のため、error return は mtr に変更を残さない
//   - 返した計画は同一 mini-transaction 内で空間管理情報 (FSP ヘッダー / xdes / inode) への
//     介在書き込みがない間のみ有効
func TouchFreeSegmentPage(mtr *buffer.Mtr, headerAt flst.Address, id page.Id) (SegmentPageFreePlan, error) {
	fileId := id.FileId()
	headerPage, err := mtr.PageForWrite(page.NewId(fileId, 0))
	if err != nil {
		return SegmentPageFreePlan{}, err
	}
	h := header{bufPage: headerPage}
	entryAddr, err := ReadSegmentHeader(mtr, fileId, headerAt)
	if err != nil {
		return SegmentPageFreePlan{}, err
	}
	entry, err := loadInodeEntryByAddress(mtr, fileId, entryAddr)
	if err != nil {
		return SegmentPageFreePlan{}, err
	}
	x, err := loadEntryByPageNumber(mtr, fileId, h, id.PageNumber())
	if err != nil {
		return SegmentPageFreePlan{}, err
	}
	pos := int(id.PageNumber() % extentPageCount)
	if x.isPageFree(pos) {
		panic(fmt.Sprintf("fsp: double free of page (pageNumber=%d)", id.PageNumber()))
	}
	state := x.state()
	switch state {
	case stateFreeFrag, stateFullFrag:
		return touchFreeSegmentFragPage(mtr, h, entry, id)
	case stateFseg, stateFsegFrag:
		return touchFreeSegmentExtentPage(mtr, fileId, h, entry, x, pos)
	default:
		panic(fmt.Sprintf("fsp: cannot free segment page in extent state %s (pageNumber=%d)", state, id.PageNumber()))
	}
}

// WriteFreeSegmentPage は TouchFreeSegmentPage で確定した計画に基づいて解放の書き込みを行う
//   - この段階では新規ページ取得を行わない (失敗する可能性がある操作は panic 変換される)
func WriteFreeSegmentPage(mtr *buffer.Mtr, plan SegmentPageFreePlan) {
	if plan.isFrag {
		writeFreeSegmentFragPage(mtr, plan)
		return
	}
	writeFreeSegmentExtentPage(mtr, plan)
}

// freeSegmentPage は entry の segment から id のページを解放する (touch + write の合成)
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
	var plan SegmentPageFreePlan
	switch state {
	case stateFreeFrag, stateFullFrag:
		plan, err = touchFreeSegmentFragPage(mtr, h, entry, id)
	case stateFseg, stateFsegFrag:
		plan, err = touchFreeSegmentExtentPage(mtr, fileId, h, entry, x, pos)
	default:
		panic(fmt.Sprintf("fsp: cannot free segment page in extent state %s (pageNumber=%d)", state, id.PageNumber()))
	}
	if err != nil {
		return err
	}
	WriteFreeSegmentPage(mtr, plan)
	return nil
}

// touchFreeSegmentFragPage は frag ページ解放の事前取得フェーズを実行する
//   - id が frag array にない場合は panic (この segment に属さないページ)
func touchFreeSegmentFragPage(mtr *buffer.Mtr, h header, entry inodeEntry, id page.Id) (SegmentPageFreePlan, error) {
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
	fragPlan, err := touchFreePage(mtr, id)
	if err != nil {
		return SegmentPageFreePlan{}, err
	}
	return SegmentPageFreePlan{
		h:        h,
		entry:    entry,
		id:       id,
		isFrag:   true,
		fragSlot: slot,
		fragPlan: fragPlan,
	}, nil
}

// touchFreeSegmentExtentPage は専有 extent 内のページ解放の事前取得フェーズを実行する
func touchFreeSegmentExtentPage(mtr *buffer.Mtr, fileId page.FileId, h header, entry inodeEntry, x xdesEntry, pos int) (SegmentPageFreePlan, error) {
	if x.segmentId() != entry.segId() {
		panic(fmt.Sprintf("fsp: extent segment id %d does not match inode segment id %d", x.segmentId(), entry.segId()))
	}
	wasFull := x.isAllUsed()
	freeAfter := x.freePageCount() + 1
	returnable := isReturnableAfterFree(x.state(), freeAfter)
	if wasFull {
		if err := touchTransitionPages(mtr, fileId, x, entry.notFullListBase()); err != nil {
			return SegmentPageFreePlan{}, err
		}
	}
	if returnable {
		destBase := spaceReturnBase(h, x.state())
		if err := touchTransitionPages(mtr, fileId, x, destBase); err != nil {
			return SegmentPageFreePlan{}, err
		}
	}
	pageNumber := extentFirstPageNumber(x) + page.PageNumber(pos)
	return SegmentPageFreePlan{
		h:          h,
		entry:      entry,
		id:         page.NewId(fileId, pageNumber),
		xdes:       x,
		pos:        pos,
		wasFull:    wasFull,
		returnable: returnable,
	}, nil
}

// writeFreeSegmentFragPage は frag ページ解放の書き込みフェーズを実行する
func writeFreeSegmentFragPage(mtr *buffer.Mtr, plan SegmentPageFreePlan) {
	plan.entry.setFragSlot(plan.fragSlot, page.MaxPageNumber)
	writeFreePage(mtr, plan.id, plan.fragPlan)
}

// writeFreeSegmentExtentPage は専有 extent 内のページ解放の書き込みフェーズを実行する
func writeFreeSegmentExtentPage(mtr *buffer.Mtr, plan SegmentPageFreePlan) {
	fileId := plan.id.FileId()
	if plan.wasFull {
		nodeAddr := plan.xdes.flstNodeAddress()
		if err := flst.Remove(mtr, fileId, plan.entry.fullListBase(), nodeAddr); err != nil {
			panicOnPostWriteFlstErr(err)
		}
		if err := flst.AddLast(mtr, fileId, plan.entry.notFullListBase(), nodeAddr); err != nil {
			panicOnPostWriteFlstErr(err)
		}
		plan.entry.setNotFullNUsed(plan.entry.notFullNUsed() + uint32(extentPageCount-1))
	} else {
		if plan.entry.notFullNUsed() == 0 {
			panic("fsp: NOT_FULL_N_USED underflow on segment page free")
		}
		plan.entry.setNotFullNUsed(plan.entry.notFullNUsed() - 1)
	}
	plan.xdes.setPageFree(plan.pos, true)
	if !plan.returnable {
		return
	}
	if plan.xdes.state() == stateFsegFrag {
		if plan.entry.notFullNUsed() == 0 {
			panic("fsp: NOT_FULL_N_USED underflow on lease extent return")
		}
		plan.entry.setNotFullNUsed(plan.entry.notFullNUsed() - 1)
	}
	if err := flst.Remove(mtr, fileId, plan.entry.notFullListBase(), plan.xdes.flstNodeAddress()); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	freeExtentToSpace(mtr, fileId, plan.h, plan.xdes)
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
