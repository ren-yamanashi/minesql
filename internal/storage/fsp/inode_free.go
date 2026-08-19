package fsp

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// freeInodeEntry は entry の inode スロットを未使用に戻し、inode ページのリスト状態を反映する
func freeInodeEntry(mtr *buffer.Mtr, fileId page.FileId, h header, entry inodeEntry) error {
	inodePage := entry.bufPage
	nodeAddr := flst.Address{PageNumber: inodePage.PageId().PageNumber(), Offset: inodePageNodeOffset}
	wasFull := findFreeInodeSlot(inodePage) < 0
	willFreePage := allOtherInodeSlotsUnused(inodePage, entry.index)
	if wasFull {
		if err := transitionInodePageFullToFree(mtr, fileId, h, nodeAddr); err != nil {
			return err
		}
	} else if willFreePage {
		if err := touchInodePageNeighbors(mtr, fileId, nodeAddr); err != nil {
			return err
		}
	}
	entry.setSegId(0)
	entry.clearMagic()
	if !willFreePage {
		return nil
	}
	if err := flst.Remove(mtr, fileId, h.segInodesFreeBase(), nodeAddr); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	return FreePage(mtr, page.NewId(fileId, inodePage.PageId().PageNumber()))
}

// transitionInodePageFullToFree は全スロット使用中だった inode ページを SEG_INODES_FULL から SEG_INODES_FREE へ移動させる
func transitionInodePageFullToFree(mtr *buffer.Mtr, fileId page.FileId, h header, nodeAddr flst.Address) error {
	last, err := flst.Last(mtr, fileId, h.segInodesFreeBase())
	if err != nil {
		return err
	}
	if !last.IsInvalid() {
		if _, err := mtr.PageForWrite(page.NewId(fileId, last.PageNumber)); err != nil {
			return err
		}
	}
	if err := flst.Remove(mtr, fileId, h.segInodesFullBase(), nodeAddr); err != nil {
		return err
	}
	if err := flst.AddLast(mtr, fileId, h.segInodesFreeBase(), nodeAddr); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	return nil
}

// allOtherInodeSlotsUnused は index 以外の全 slot が未使用 (segId == 0) かを返す
func allOtherInodeSlotsUnused(inodePage *buffer.Page, index int) bool {
	for i := range inodeEntriesPerPage {
		if i == index {
			continue
		}
		e := inodeEntry{bufPage: inodePage, index: i}
		if e.segId() != 0 {
			return false
		}
	}
	return true
}

// touchInodePageNeighbors は nodeAddr の SEG_INODES_FREE 上の前後ページを事前タッチする
func touchInodePageNeighbors(mtr *buffer.Mtr, fileId page.FileId, nodeAddr flst.Address) error {
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
	return nil
}
