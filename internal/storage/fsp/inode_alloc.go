package fsp

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// allocateInodeEntry は未使用の inode スロットを 1 つ確保し、そのアクセサを返す
//   - SEG_INODES_FREE が空なら新しい inode ページを確保して SEG_INODES_FREE に追加する
//   - 全スロット使用中になった場合は SEG_INODES_FREE から SEG_INODES_FULL へ移す
//   - 呼び出し側が initializeEntry を呼んでスロット内容を初期化する
func allocateInodeEntry(mtr *buffer.Mtr, fileId page.FileId, h header) (inodeEntry, error) {
	if err := ensureFreeInodePage(mtr, fileId, h); err != nil {
		return inodeEntry{}, err
	}
	freeFirst, err := flst.First(mtr, fileId, h.segInodesFreeBase())
	if err != nil {
		return inodeEntry{}, err
	}
	inodePage, err := mtr.PageForWrite(page.NewId(fileId, freeFirst.PageNumber))
	if err != nil {
		return inodeEntry{}, err
	}
	slot := findFreeInodeSlot(inodePage)
	if slot < 0 {
		panic(fmt.Sprintf("fsp: SEG_INODES_FREE head page has no free slot (pageNumber=%d)", freeFirst.PageNumber))
	}
	entry := inodeEntry{bufPage: inodePage, index: slot}
	if !hasAnotherFreeSlot(inodePage, slot) {
		if err := transitionInodePageFreeToFull(mtr, fileId, h, freeFirst); err != nil {
			return inodeEntry{}, err
		}
	}
	return entry, nil
}

// loadInodeEntryByAddress はエントリアドレスから対応する inodeEntry を復元する
//   - offset が inode エントリ配列の境界に整合しない場合は panic する
//
//nolint:unparam // fileId is part of the accessor API and receives arbitrary file ids from external callers
func loadInodeEntryByAddress(mtr *buffer.Mtr, fileId page.FileId, addr flst.Address) (inodeEntry, error) {
	bufPage, err := mtr.PageForWrite(page.NewId(fileId, addr.PageNumber))
	if err != nil {
		return inodeEntry{}, err
	}
	rel := int(addr.Offset) - inodeArrOffset
	if rel < 0 || rel%inodeEntrySize != 0 || rel/inodeEntrySize >= inodeEntriesPerPage {
		panic(fmt.Sprintf("fsp: invalid inode entry address offset=%d", addr.Offset))
	}
	return inodeEntry{bufPage: bufPage, index: rel / inodeEntrySize}, nil
}

// ensureFreeInodePage は SEG_INODES_FREE が空なら新しい inode ページを確保して追加する
func ensureFreeInodePage(mtr *buffer.Mtr, fileId page.FileId, h header) error {
	length, err := flst.Length(mtr, fileId, h.segInodesFreeBase())
	if err != nil {
		return err
	}
	if length > 0 {
		return nil
	}
	pageId, err := AllocatePage(mtr, fileId)
	if err != nil {
		return err
	}
	if _, err := mtr.Pool().AddPage(pageId); err != nil {
		return err
	}
	inodePage, err := mtr.PageForWrite(pageId)
	if err != nil {
		return err
	}
	for i := range inodeEntriesPerPage {
		(inodeEntry{bufPage: inodePage, index: i}).setSegId(0)
	}
	nodeAddr := flst.Address{PageNumber: pageId.PageNumber(), Offset: inodePageNodeOffset}
	if err := flst.AddLast(mtr, fileId, h.segInodesFreeBase(), nodeAddr); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	return nil
}

// findFreeInodeSlot は inode ページ内で segId == 0 の最小 index を返す (満杯なら -1)
func findFreeInodeSlot(inodePage *buffer.Page) int {
	for i := range inodeEntriesPerPage {
		entry := inodeEntry{bufPage: inodePage, index: i}
		if entry.segId() == 0 {
			return i
		}
	}
	return -1
}

// hasAnotherFreeSlot は inode ページ内に allocated 以外の未使用 slot が残っているかを返す
func hasAnotherFreeSlot(inodePage *buffer.Page, allocated int) bool {
	for i := range inodeEntriesPerPage {
		if i == allocated {
			continue
		}
		entry := inodeEntry{bufPage: inodePage, index: i}
		if entry.segId() == 0 {
			return true
		}
	}
	return false
}

// transitionInodePageFreeToFull は全スロット使用中になった inode ページを SEG_INODES_FREE から SEG_INODES_FULL へ移動させる
func transitionInodePageFreeToFull(mtr *buffer.Mtr, fileId page.FileId, h header, nodeAddr flst.Address) error {
	if err := flst.Remove(mtr, fileId, h.segInodesFreeBase(), nodeAddr); err != nil {
		return err
	}
	if err := flst.AddLast(mtr, fileId, h.segInodesFullBase(), nodeAddr); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	return nil
}
