package fsp

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// acquireExtent は segment に extent を 1 つ獲得して inode のリストへ繋ぐ
//   - lease 試行 → 空間からの新規確保 (+ フリーリスト先読み) の順。呼び出し前提: segment の NOT_FULL / FREE がともに空
func acquireExtent(mtr *buffer.Mtr, fileId page.FileId, h header, entry inodeEntry) error {
	leased, err := tryLeaseFragExtent(mtr, fileId, h, entry)
	if err != nil {
		return err
	}
	if leased {
		return nil
	}
	x, err := allocateExtent(mtr, fileId, h)
	if err != nil {
		return err
	}
	x.setSegmentId(entry.segId())
	x.setState(stateFseg)
	if err := flst.AddLast(mtr, fileId, entry.freeListBase(), x.flstNodeAddress()); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	return fillSegmentFreeList(mtr, fileId, h, entry)
}

// tryLeaseFragExtent は空間 FREE_FRAG 末尾の extent の lease を試みる (成立したら true)
func tryLeaseFragExtent(mtr *buffer.Mtr, fileId page.FileId, h header, entry inodeEntry) (bool, error) {
	last, err := flst.Last(mtr, fileId, h.freeFragListBase())
	if err != nil {
		return false, err
	}
	if last.IsInvalid() {
		return false, nil
	}
	x, err := loadEntryByNodeAddress(mtr, fileId, last)
	if err != nil {
		return false, err
	}
	if !x.isLeasable() {
		return false, nil
	}
	if err := flst.Remove(mtr, fileId, h.freeFragListBase(), last); err != nil {
		return false, err
	}
	h.setFragNUsed(h.fragNUsed() - 1)
	x.setSegmentId(entry.segId())
	x.setState(stateFsegFrag)
	if err := flst.AddLast(mtr, fileId, entry.notFullListBase(), x.flstNodeAddress()); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	entry.setNotFullNUsed(entry.notFullNUsed() + 1)
	return true, nil
}

// fillSegmentFreeList は予約総ページ数が閾値以上なら extent を最大 segFillAddExtents 個先読み確保して segment の FREE リストへ繋ぐ
func fillSegmentFreeList(mtr *buffer.Mtr, fileId page.FileId, h header, entry inodeEntry) error {
	reserved, _, err := segmentReservedPages(mtr, fileId, entry)
	if err != nil {
		return err
	}
	if reserved < segFillReservedExtents*extentPageCount {
		return nil
	}
	for range segFillAddExtents {
		x, err := allocateExtent(mtr, fileId, h)
		if err != nil {
			if errors.Is(err, errCapacityExhausted) {
				return nil
			}
			return err
		}
		x.setSegmentId(entry.segId())
		x.setState(stateFseg)
		if err := flst.AddLast(mtr, fileId, entry.freeListBase(), x.flstNodeAddress()); err != nil {
			panicOnPostWriteFlstErr(err)
		}
	}
	return nil
}
