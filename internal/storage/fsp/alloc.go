package fsp

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// AllocatePage は fileId のファイルからページを 1 つ割り当て、その PageId を返す
//   - 採番と xdes / リスト / FRAG_N_USED の更新までを行う。バッファページの作成と内容初期化は呼び出し側の責務
func AllocatePage(mtr *buffer.Mtr, fileId page.FileId) (page.Id, error) {
	headerPage, err := mtr.PageForWrite(page.NewId(fileId, 0))
	if err != nil {
		return page.InvalidId(), err
	}
	h := header{bufPage: headerPage}
	entry, err := prepareAllocationExtent(mtr, fileId, h)
	if err != nil {
		return page.InvalidId(), err
	}
	pos := entry.firstFreePos()
	if pos < 0 {
		panic("fsp: FREE_FRAG extent has no free page")
	}
	willBeFull := entry.freePageCount() == 1
	if willBeFull {
		if err := touchFreeFragToFullFragPages(mtr, fileId, h, entry); err != nil {
			return page.InvalidId(), err
		}
	}
	entry.setPageFree(pos, false)
	h.setFragNUsed(h.fragNUsed() + 1)
	if entry.isAllUsed() {
		transitionFreeFragToFullFrag(mtr, fileId, h, entry)
	}
	pageNumber := extentFirstPageNumber(entry) + page.PageNumber(pos)
	return page.NewId(fileId, pageNumber), nil
}

// prepareAllocationExtent は割り当てに使う FREE_FRAG リスト先頭 extent を用意して返す
func prepareAllocationExtent(mtr *buffer.Mtr, fileId page.FileId, h header) (xdesEntry, error) {
	for {
		first, err := flst.First(mtr, fileId, h.freeFragListBase())
		if err != nil {
			return xdesEntry{}, err
		}
		if !first.IsInvalid() {
			return loadEntryByNodeAddress(mtr, fileId, first)
		}
		firstFree, err := flst.First(mtr, fileId, h.freeListBase())
		if err != nil {
			return xdesEntry{}, err
		}
		if !firstFree.IsInvalid() {
			if err := transitionFreeToFreeFrag(mtr, fileId, h, firstFree); err != nil {
				return xdesEntry{}, err
			}
			continue
		}
		if err := fill(mtr, fileId); err != nil {
			return xdesEntry{}, err
		}
	}
}

// transitionFreeToFreeFrag は FREE リストの extent を FREE_FRAG リストへ移動させる
func transitionFreeToFreeFrag(mtr *buffer.Mtr, fileId page.FileId, h header, nodeAddr flst.Address) error {
	entry, err := loadEntryByNodeAddress(mtr, fileId, nodeAddr)
	if err != nil {
		return err
	}
	if err := flst.Remove(mtr, fileId, h.freeListBase(), nodeAddr); err != nil {
		return err
	}
	entry.setState(stateFreeFrag)
	if err := flst.AddLast(mtr, fileId, h.freeFragListBase(), nodeAddr); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	return nil
}

// transitionFreeFragToFullFrag は満杯になった extent を FREE_FRAG から FULL_FRAG へ移動させ FRAG_N_USED を減算する
//   - 呼び出し前に touchFreeFragToFullFragPages で全ページを取得済みであることを前提とする
func transitionFreeFragToFullFrag(mtr *buffer.Mtr, fileId page.FileId, h header, entry xdesEntry) {
	nodeAddr := entry.flstNodeAddress()
	if err := flst.Remove(mtr, fileId, h.freeFragListBase(), nodeAddr); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	entry.setState(stateFullFrag)
	if err := flst.AddLast(mtr, fileId, h.fullFragListBase(), nodeAddr); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	h.setFragNUsed(h.fragNUsed() - uint32(extentPageCount))
}

// touchFreeFragToFullFragPages は満杯遷移で必要になる全ページを事前取得する
//   - FREE_FRAG リスト上の entry の前後 node と FULL_FRAG リストの旧末尾を対象とする
func touchFreeFragToFullFragPages(mtr *buffer.Mtr, fileId page.FileId, h header, entry xdesEntry) error {
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
	last, err := flst.Last(mtr, fileId, h.fullFragListBase())
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

// panicOnPostWriteFlstErr は不可分単位の書き込み開始後に flst 呼び出しが失敗した場合の到達不能 assert
func panicOnPostWriteFlstErr(err error) {
	panic(fmt.Sprintf("fsp: flst update after write start must not fail: %v", err))
}

// loadEntryByNodeAddress は node の Address から対応する xdesEntry を復元する
func loadEntryByNodeAddress(mtr *buffer.Mtr, fileId page.FileId, addr flst.Address) (xdesEntry, error) {
	bufPage, err := mtr.PageForWrite(page.NewId(fileId, addr.PageNumber))
	if err != nil {
		return xdesEntry{}, err
	}
	rel := int(addr.Offset) - headerSize - xdesFlstNodeOffset
	if rel < 0 || rel%xdesEntrySize != 0 || rel/xdesEntrySize >= descriptorEntriesPerPage {
		panic(fmt.Sprintf("fsp: invalid xdes node address offset=%d", addr.Offset))
	}
	return xdesEntry{bufPage: bufPage, index: rel / xdesEntrySize}, nil
}

// extentFirstPageNumber は entry が担当する extent の先頭 PageNumber を返す
func extentFirstPageNumber(entry xdesEntry) page.PageNumber {
	return entry.bufPage.PageId().PageNumber() + page.PageNumber(entry.index*extentPageCount)
}
