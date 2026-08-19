package fsp

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// FreeSegmentStep は segment の解放を 1 単位 (extent 1 つ、または frag ページ 1 枚) 進める
//   - 解放が完了した (または既に完了していた) 場合は true を返す
func FreeSegmentStep(mtr *buffer.Mtr, fileId page.FileId, headerAt flst.Address) (bool, error) {
	done, err := IsPageFree(mtr, page.NewId(fileId, headerAt.PageNumber))
	if err != nil {
		return false, err
	}
	if done {
		return true, nil
	}
	entryAddr, err := ReadSegmentHeader(mtr, fileId, headerAt)
	if err != nil {
		return false, err
	}
	entry, err := loadInodeEntryByAddress(mtr, fileId, entryAddr)
	if err != nil {
		return false, err
	}
	if entry.segId() == 0 {
		return true, nil
	}
	headerPage, err := mtr.PageForWrite(page.NewId(fileId, 0))
	if err != nil {
		return false, err
	}
	h := header{bufPage: headerPage}
	x, ok, err := firstExtentOfSegment(mtr, fileId, entry)
	if err != nil {
		return false, err
	}
	if ok {
		if err := freeWholeExtent(mtr, fileId, h, entry, x); err != nil {
			return false, err
		}
		return false, nil
	}
	slot := entry.lastUsedFragSlot()
	if slot < 0 {
		if err := freeInodeEntry(mtr, fileId, h, entry); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := freeSegmentPage(mtr, h, entry, page.NewId(fileId, entry.fragSlot(slot))); err != nil {
		return false, err
	}
	if entry.lastUsedFragSlot() >= 0 {
		return false, nil
	}
	if err := freeInodeEntry(mtr, fileId, h, entry); err != nil {
		return false, err
	}
	return true, nil
}

// firstExtentOfSegment は解放対象の extent を FULL → NOT_FULL → FREE の順の先頭から返す (なければ ok = false)
func firstExtentOfSegment(mtr *buffer.Mtr, fileId page.FileId, entry inodeEntry) (xdesEntry, bool, error) {
	bases := []flst.Address{entry.fullListBase(), entry.notFullListBase(), entry.freeListBase()}
	for _, base := range bases {
		first, err := flst.First(mtr, fileId, base)
		if err != nil {
			return xdesEntry{}, false, err
		}
		if first.IsInvalid() {
			continue
		}
		x, err := loadEntryByNodeAddress(mtr, fileId, first)
		if err != nil {
			return xdesEntry{}, false, err
		}
		return x, true, nil
	}
	return xdesEntry{}, false, nil
}

// freeWholeExtent は extent 1 つを segment から丸ごと解放して空間へ返却する
func freeWholeExtent(mtr *buffer.Mtr, fileId page.FileId, h header, entry inodeEntry, x xdesEntry) error {
	state := x.state()
	if state != stateFseg && state != stateFsegFrag {
		panic(fmt.Sprintf("fsp: cannot free whole extent in state %s", state))
	}
	if x.segmentId() != entry.segId() {
		panic(fmt.Sprintf("fsp: extent segment id %d does not match inode segment id %d", x.segmentId(), entry.segId()))
	}
	srcBase, usedInNotFull := segmentExtentSourceBase(entry, x)
	destBase := spaceReturnBase(h, state)
	if err := touchTransitionPages(mtr, fileId, x, destBase); err != nil {
		return err
	}
	if usedInNotFull > 0 {
		if entry.notFullNUsed() < uint32(usedInNotFull) {
			panic("fsp: NOT_FULL_N_USED underflow on whole extent free")
		}
		entry.setNotFullNUsed(entry.notFullNUsed() - uint32(usedInNotFull))
	}
	if err := flst.Remove(mtr, fileId, srcBase, x.flstNodeAddress()); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	return freeExtentToSpace(mtr, fileId, h, x)
}

// segmentExtentSourceBase は x が所属している segment 側リストの base と、NOT_FULL の場合の使用中ページ数を返す
func segmentExtentSourceBase(entry inodeEntry, x xdesEntry) (flst.Address, int) {
	switch {
	case x.isAllUsed():
		return entry.fullListBase(), 0
	case x.isAllFree():
		return entry.freeListBase(), 0
	default:
		return entry.notFullListBase(), extentPageCount - x.freePageCount()
	}
}
