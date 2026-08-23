package fsp

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// freePagePlan は FreePage の書き込みフェーズが必要とする事前情報
type freePagePlan struct {
	h              header
	entry          xdesEntry
	pos            int
	state          extentState
	willBecomeFree bool
}

// FreePage は id のページを解放し、xdes / リスト / FRAG_N_USED を更新する
//   - 解放対象ページの中身には書き込まない
//   - 二重解放、FREE / NOT_INITED 状態への解放、フリーリミット以上のページ番号は panic
func FreePage(mtr *buffer.Mtr, id page.Id) error {
	plan, err := touchFreePage(mtr, id)
	if err != nil {
		return err
	}
	writeFreePage(mtr, id, plan)
	return nil
}

// IsPageFree は id のページが free かどうかを返す
//   - フリーリミット以上のページは常に true を返す
func IsPageFree(mtr *buffer.Mtr, id page.Id) (bool, error) {
	fileId := id.FileId()
	headerPage, err := mtr.PageForRead(page.NewId(fileId, 0))
	if err != nil {
		return false, err
	}
	h := header{bufPage: headerPage}
	if id.PageNumber() >= h.freeLimit() {
		return true, nil
	}
	descrPage, err := mtr.PageForRead(page.NewId(fileId, descriptorPageNumber(id.PageNumber())))
	if err != nil {
		return false, err
	}
	entry := xdesEntry{bufPage: descrPage, index: descriptorEntryIndex(id.PageNumber())}
	return entry.isPageFree(int(id.PageNumber() % extentPageCount)), nil
}

// touchFreePage は FreePage の書き込みフェーズが必要とする全ページを pin し、遷移計画を返す
//   - 状態 / bit を検査して二重解放や不正状態を panic で検出する
//   - 呼び出し後は書き込み前のため、error return は mtr に変更を残さない
func touchFreePage(mtr *buffer.Mtr, id page.Id) (freePagePlan, error) {
	fileId := id.FileId()
	headerPage, err := mtr.PageForWrite(page.NewId(fileId, 0))
	if err != nil {
		return freePagePlan{}, err
	}
	h := header{bufPage: headerPage}
	entry, err := loadEntryByPageNumber(mtr, fileId, h, id.PageNumber())
	if err != nil {
		return freePagePlan{}, err
	}
	pos := int(id.PageNumber() % extentPageCount)
	state := entry.state()
	if state != stateFreeFrag && state != stateFullFrag {
		panic(fmt.Sprintf("fsp: cannot free page in extent state %s (pageNumber=%d)", state, id.PageNumber()))
	}
	if entry.isPageFree(pos) {
		panic(fmt.Sprintf("fsp: double free of page (pageNumber=%d)", id.PageNumber()))
	}
	willBecomeFree := state == stateFreeFrag && entry.freePageCount() == extentPageCount-1
	switch {
	case state == stateFullFrag:
		if err := touchTransitionPages(mtr, fileId, entry, h.freeFragListBase()); err != nil {
			return freePagePlan{}, err
		}
	case willBecomeFree:
		if err := touchTransitionPages(mtr, fileId, entry, h.freeListBase()); err != nil {
			return freePagePlan{}, err
		}
	}
	return freePagePlan{h: h, entry: entry, pos: pos, state: state, willBecomeFree: willBecomeFree}, nil
}

// writeFreePage は touchFreePage で確定した計画に基づいて xdes / リスト / FRAG_N_USED を更新する
//   - この段階では新規ページ取得を行わない (失敗する可能性がある操作は panic 変換される)
func writeFreePage(mtr *buffer.Mtr, id page.Id, plan freePagePlan) {
	fileId := id.FileId()
	plan.entry.setPageFree(plan.pos, true)
	if plan.state == stateFullFrag {
		transitionFullFragToFreeFrag(mtr, fileId, plan.h, plan.entry)
	} else {
		if plan.h.fragNUsed() == 0 {
			panic("fsp: FRAG_N_USED underflow on free in FREE_FRAG extent")
		}
		plan.h.setFragNUsed(plan.h.fragNUsed() - 1)
	}
	if plan.willBecomeFree {
		transitionFreeFragToFree(mtr, fileId, plan.h, plan.entry)
	}
}

// loadEntryByPageNumber は pageNumber を担当する xdesEntry を書き込み用に取得する
//   - pageNumber がフリーリミット以上の場合は panic
func loadEntryByPageNumber(mtr *buffer.Mtr, fileId page.FileId, h header, pageNumber page.PageNumber) (xdesEntry, error) {
	if pageNumber >= h.freeLimit() {
		panic(fmt.Sprintf("fsp: page number %d beyond free limit %d", pageNumber, h.freeLimit()))
	}
	descrPage, err := mtr.PageForWrite(page.NewId(fileId, descriptorPageNumber(pageNumber)))
	if err != nil {
		return xdesEntry{}, err
	}
	return xdesEntry{bufPage: descrPage, index: descriptorEntryIndex(pageNumber)}, nil
}

// touchTransitionPages は状態遷移で必要になる全ページを事前取得する
//   - 現在所属しているリスト上の entry の前後 node と、遷移先リストの旧末尾を対象とする
func touchTransitionPages(mtr *buffer.Mtr, fileId page.FileId, entry xdesEntry, destBase flst.Address) error {
	nodeAddr := entry.flstNodeAddress()
	prev, err := flst.Prev(mtr, fileId, nodeAddr)
	if err != nil {
		return err
	}
	if !prev.IsInvalid() {
		if _, err := mtr.PageForWrite(page.NewId(fileId, prev.PageNumber)); err != nil {
			return err
		}
	}
	next, err := flst.Next(mtr, fileId, nodeAddr)
	if err != nil {
		return err
	}
	if !next.IsInvalid() {
		if _, err := mtr.PageForWrite(page.NewId(fileId, next.PageNumber)); err != nil {
			return err
		}
	}
	last, err := flst.Last(mtr, fileId, destBase)
	if err != nil {
		return err
	}
	if !last.IsInvalid() {
		if _, err := mtr.PageForWrite(page.NewId(fileId, last.PageNumber)); err != nil {
			return err
		}
	}
	return nil
}

// transitionFullFragToFreeFrag は 1 ページ解放で満杯でなくなった extent を FULL_FRAG から FREE_FRAG へ移動させ FRAG_N_USED を加算する
//   - 呼び出し前に touchTransitionPages で全ページを取得済みであることを前提とする
func transitionFullFragToFreeFrag(mtr *buffer.Mtr, fileId page.FileId, h header, entry xdesEntry) {
	nodeAddr := entry.flstNodeAddress()
	if err := flst.Remove(mtr, fileId, h.fullFragListBase(), nodeAddr); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	entry.setState(stateFreeFrag)
	if err := flst.AddLast(mtr, fileId, h.freeFragListBase(), nodeAddr); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	h.setFragNUsed(h.fragNUsed() + uint32(extentPageCount-1))
}

// transitionFreeFragToFree は全ページ free になった extent を FREE_FRAG から FREE へ移動させる
//   - 呼び出し前に touchTransitionPages で全ページを取得済みであることを前提とする
func transitionFreeFragToFree(mtr *buffer.Mtr, fileId page.FileId, h header, entry xdesEntry) {
	nodeAddr := entry.flstNodeAddress()
	if err := flst.Remove(mtr, fileId, h.freeFragListBase(), nodeAddr); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	entry.setState(stateFree)
	if err := flst.AddLast(mtr, fileId, h.freeListBase(), nodeAddr); err != nil {
		panicOnPostWriteFlstErr(err)
	}
}

// freeExtentToSpace は segment から返却された extent を空間のリストへ戻す
//   - XDES_FSEG は全ページを free 化して FREE リストへ、XDES_FSEG_FRAG は予約分 (先頭 1 ページ) を
//     使用中に戻して FREE_FRAG リストの末尾へ繋ぎ、FRAG_N_USED に予約分 1 を加算する
//   - 呼び出し前提: extent は segment 側リストから除去済みであり、遷移先リスト (freeListBase または
//     freeFragListBase) の旧末尾ページは呼び出し側が touchTransitionPages 等で pin 済みであること
func freeExtentToSpace(mtr *buffer.Mtr, fileId page.FileId, h header, x xdesEntry) {
	state := x.state()
	if state != stateFseg && state != stateFsegFrag {
		panic(fmt.Sprintf("fsp: cannot return extent to space in state %s", state))
	}
	var destBase flst.Address
	if state == stateFseg {
		destBase = h.freeListBase()
	} else {
		destBase = h.freeFragListBase()
	}
	x.setSegmentId(0)
	x.setAllPagesFree()
	if state == stateFseg {
		x.setState(stateFree)
	} else {
		x.setPageFree(0, false)
		x.setState(stateFreeFrag)
	}
	if err := flst.AddLast(mtr, fileId, destBase, x.flstNodeAddress()); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	if state == stateFsegFrag {
		h.setFragNUsed(h.fragNUsed() + 1)
	}
}
