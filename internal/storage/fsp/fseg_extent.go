package fsp

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// acquireExtent は segment に extent を 1 つ獲得し、その xdesEntry を返す
//   - lease 試行 → 空間からの新規確保 (+ フリーリスト先読み) の順
//   - 呼び出し前提: segment の NOT_FULL / FREE がともに空であること
func acquireExtent(mtr *buffer.Mtr, fileId page.FileId, h header, entry inodeEntry) (xdesEntry, error) {
	preReserved, _, err := segmentReservedPages(mtr, fileId, entry)
	if err != nil {
		return xdesEntry{}, err
	}
	leased, x, err := tryLeaseFragExtent(mtr, fileId, h, entry)
	if err != nil {
		return xdesEntry{}, err
	}
	if leased {
		return x, nil
	}
	x, err = allocateExtent(mtr, fileId, h)
	if err != nil {
		return xdesEntry{}, err
	}
	x.setSegmentId(entry.segId())
	x.setState(stateFseg)
	if err := flst.AddLast(mtr, fileId, entry.freeListBase(), x.flstNodeAddress()); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	fillSegmentFreeList(mtr, fileId, h, entry, preReserved+extentPageCount)
	return x, nil
}

// tryLeaseFragExtent は空間 FREE_FRAG 末尾の extent の lease を試みる
//   - lease が成立した場合は (true, xdesEntry) を返す
func tryLeaseFragExtent(mtr *buffer.Mtr, fileId page.FileId, h header, entry inodeEntry) (bool, xdesEntry, error) {
	last, err := flst.Last(mtr, fileId, h.freeFragListBase())
	if err != nil {
		return false, xdesEntry{}, err
	}
	if last.IsInvalid() {
		return false, xdesEntry{}, nil
	}
	x, err := loadEntryByNodeAddress(mtr, fileId, last)
	if err != nil {
		return false, xdesEntry{}, err
	}
	if !x.isLeasable() {
		return false, xdesEntry{}, nil
	}
	if err := flst.Remove(mtr, fileId, h.freeFragListBase(), last); err != nil {
		return false, xdesEntry{}, err
	}
	h.setFragNUsed(h.fragNUsed() - 1)
	x.setSegmentId(entry.segId())
	x.setState(stateFsegFrag)
	if err := flst.AddLast(mtr, fileId, entry.notFullListBase(), x.flstNodeAddress()); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	entry.setNotFullNUsed(entry.notFullNUsed() + 1)
	return true, x, nil
}

// fillSegmentFreeList は予約総ページ数が閾値以上なら extent を最大 segFillAddExtents 個先読み確保して segment の FREE リストへ繋ぐ
//   - reserved は呼び出し側が書き込み開始前に事前計算した値
//   - 先読み中に extent を確保できない場合は best-effort で打ち切る
func fillSegmentFreeList(mtr *buffer.Mtr, fileId page.FileId, h header, entry inodeEntry, reserved int) {
	if reserved < segFillReservedExtents*extentPageCount {
		return
	}
	for range segFillAddExtents {
		x, err := allocateExtent(mtr, fileId, h)
		if err != nil {
			return
		}
		x.setSegmentId(entry.segId())
		x.setState(stateFseg)
		if err := flst.AddLast(mtr, fileId, entry.freeListBase(), x.flstNodeAddress()); err != nil {
			panicOnPostWriteFlstErr(err)
		}
	}
}
