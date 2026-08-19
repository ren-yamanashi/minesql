package fsp

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// AllocateSegmentPage は headerAt の segment header が指す segment にページを 1 つ割り当て、その PageId を返す
//   - xdes / リスト / inode の更新までを行う。バッファページの作成と内容初期化は呼び出し側の責務
func AllocateSegmentPage(mtr *buffer.Mtr, fileId page.FileId, headerAt flst.Address) (page.Id, error) {
	entryAddr, err := ReadSegmentHeader(mtr, fileId, headerAt)
	if err != nil {
		return page.InvalidId(), err
	}
	entry, err := loadInodeEntryByAddress(mtr, fileId, entryAddr)
	if err != nil {
		return page.InvalidId(), err
	}
	headerPage, err := mtr.PageForWrite(page.NewId(fileId, 0))
	if err != nil {
		return page.InvalidId(), err
	}
	h := header{bufPage: headerPage}
	return allocatePageInSegment(mtr, fileId, h, entry)
}

// allocatePageInSegment は entry の segment にページを 1 つ割り当てる
func allocatePageInSegment(mtr *buffer.Mtr, fileId page.FileId, h header, entry inodeEntry) (page.Id, error) {
	for {
		x, ok, err := pickAllocatableExtent(mtr, fileId, entry)
		if err != nil {
			return page.InvalidId(), err
		}
		if ok {
			pos := x.firstFreePos()
			if pos < 0 {
				panic("fsp: segment extent on NOT_FULL/FREE has no free page")
			}
			if err := markSegmentPageUsed(mtr, fileId, entry, x, pos); err != nil {
				return page.InvalidId(), err
			}
			return page.NewId(fileId, extentFirstPageNumber(x)+page.PageNumber(pos)), nil
		}
		_, used, err := segmentReservedPages(mtr, fileId, entry)
		if err != nil {
			return page.InvalidId(), err
		}
		if used < inodeFragSlotCount {
			pageId, err := AllocatePage(mtr, fileId)
			if err != nil {
				return page.InvalidId(), err
			}
			slot := entry.firstFreeFragSlot()
			if slot < 0 {
				panic("fsp: frag array full while used < inodeFragSlotCount")
			}
			entry.setFragSlot(slot, pageId.PageNumber())
			return pageId, nil
		}
		if err := acquireExtent(mtr, fileId, h, entry); err != nil {
			return page.InvalidId(), err
		}
	}
}

// pickAllocatableExtent は NOT_FULL 先頭 → FREE 先頭の順で割り当て対象 extent を返す (どちらも空なら ok = false)
func pickAllocatableExtent(mtr *buffer.Mtr, fileId page.FileId, entry inodeEntry) (xdesEntry, bool, error) {
	first, err := flst.First(mtr, fileId, entry.notFullListBase())
	if err != nil {
		return xdesEntry{}, false, err
	}
	if !first.IsInvalid() {
		x, err := loadEntryByNodeAddress(mtr, fileId, first)
		if err != nil {
			return xdesEntry{}, false, err
		}
		return x, true, nil
	}
	first, err = flst.First(mtr, fileId, entry.freeListBase())
	if err != nil {
		return xdesEntry{}, false, err
	}
	if !first.IsInvalid() {
		x, err := loadEntryByNodeAddress(mtr, fileId, first)
		if err != nil {
			return xdesEntry{}, false, err
		}
		return x, true, nil
	}
	return xdesEntry{}, false, nil
}

// markSegmentPageUsed は extent 内ページ位置 pos を使用中にし、リスト遷移と NOT_FULL_N_USED を反映する
func markSegmentPageUsed(mtr *buffer.Mtr, fileId page.FileId, entry inodeEntry, x xdesEntry, pos int) error {
	if x.isAllFree() {
		if err := touchTransitionPages(mtr, fileId, x, entry.notFullListBase()); err != nil {
			return err
		}
		nodeAddr := x.flstNodeAddress()
		if err := flst.Remove(mtr, fileId, entry.freeListBase(), nodeAddr); err != nil {
			panicOnPostWriteFlstErr(err)
		}
		if err := flst.AddLast(mtr, fileId, entry.notFullListBase(), nodeAddr); err != nil {
			panicOnPostWriteFlstErr(err)
		}
	}
	willBeFull := x.freePageCount() == 1
	if willBeFull {
		if err := touchTransitionPages(mtr, fileId, x, entry.fullListBase()); err != nil {
			return err
		}
	}
	x.setPageFree(pos, false)
	entry.setNotFullNUsed(entry.notFullNUsed() + 1)
	if x.isAllUsed() {
		nodeAddr := x.flstNodeAddress()
		if err := flst.Remove(mtr, fileId, entry.notFullListBase(), nodeAddr); err != nil {
			panicOnPostWriteFlstErr(err)
		}
		if err := flst.AddLast(mtr, fileId, entry.fullListBase(), nodeAddr); err != nil {
			panicOnPostWriteFlstErr(err)
		}
		entry.setNotFullNUsed(entry.notFullNUsed() - uint32(extentPageCount))
	}
	return nil
}

// segmentReservedPages は segment の予約総ページ数と使用中ページ数を返す
func segmentReservedPages(mtr *buffer.Mtr, fileId page.FileId, entry inodeEntry) (reserved, used int, err error) {
	frags := entry.fragPageCount()
	fullLen, err := flst.Length(mtr, fileId, entry.fullListBase())
	if err != nil {
		return 0, 0, err
	}
	notFullLen, err := flst.Length(mtr, fileId, entry.notFullListBase())
	if err != nil {
		return 0, 0, err
	}
	freeLen, err := flst.Length(mtr, fileId, entry.freeListBase())
	if err != nil {
		return 0, 0, err
	}
	notFullUsed := int(entry.notFullNUsed())
	used = frags + int(fullLen)*extentPageCount + notFullUsed
	reserved = frags + (int(fullLen)+int(notFullLen)+int(freeLen))*extentPageCount
	return reserved, used, nil
}
