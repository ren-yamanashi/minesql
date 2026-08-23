package fsp

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// CreateSegment は新しい segment を作成し、segment 自身から最初の 1 ページを割り当てて
// そのページの body 先頭から headerOffset の位置に segment header を書き込み、そのページの Id を返す
//   - 割り当てたページはバッファプールに作成される。segment header 以外の内容初期化は呼び出し側の責務
//   - 最初のページ割り当てに失敗した場合は、確保した inode スロットを解放してから失敗を返す
//     (segment id カウンタは戻さない)
func CreateSegment(mtr *buffer.Mtr, fileId page.FileId, headerOffset uint16) (page.Id, error) {
	headerPage, err := mtr.PageForWrite(page.NewId(fileId, 0))
	if err != nil {
		return page.InvalidId(), err
	}
	h := header{bufPage: headerPage}
	entry, err := createInode(mtr, fileId)
	if err != nil {
		return page.InvalidId(), err
	}
	firstPageId, err := allocatePageInSegment(mtr, fileId, h, entry)
	if err != nil {
		if compErr := freeInodeEntry(mtr, fileId, h, entry); compErr != nil {
			panic(fmt.Sprintf("fsp: inode compensation failed after CreateSegment first-page allocation: %v", compErr))
		}
		return page.InvalidId(), err
	}
	if _, err := mtr.Pool().AddPage(firstPageId); err != nil {
		panic(fmt.Sprintf("fsp: pool AddPage failed for just-allocated first page (pageId=%v): %v", firstPageId, err))
	}
	if err := writeSegmentHeader(mtr, firstPageId, headerOffset, entry.address()); err != nil {
		panic(fmt.Sprintf("fsp: writeSegmentHeader failed for just-allocated first page (pageId=%v): %v", firstPageId, err))
	}
	return firstPageId, nil
}

// CreateSegmentAt は新しい segment を作成し、既存ページ上の headerAt に segment header を書き込む
//   - 最初のページは割り当てない (segment header を別 segment のページに置く利用者向け)
func CreateSegmentAt(mtr *buffer.Mtr, fileId page.FileId, headerAt flst.Address) error {
	headerPage, err := mtr.PageForWrite(page.NewId(fileId, headerAt.PageNumber))
	if err != nil {
		return err
	}
	entry, err := createInode(mtr, fileId)
	if err != nil {
		return err
	}
	writeAddressToPage(headerPage, int(headerAt.Offset), entry.address())
	return nil
}

// ReadSegmentHeader は headerAt に置かれた segment header を読み取り、inode エントリのアドレスを返す
func ReadSegmentHeader(mtr *buffer.Mtr, fileId page.FileId, headerAt flst.Address) (flst.Address, error) {
	bufPage, err := mtr.PageForRead(page.NewId(fileId, headerAt.PageNumber))
	if err != nil {
		return flst.Address{}, err
	}
	return flst.ReadAddress(bufPage.Data().Body(), int(headerAt.Offset)), nil
}

// createInode は inode スロットを確保して初期化し、segment id を採番する
//   - allocateInodeEntry が失敗した場合は segment id カウンタを進めずに失敗を返す
func createInode(mtr *buffer.Mtr, fileId page.FileId) (inodeEntry, error) {
	headerPage, err := mtr.PageForWrite(page.NewId(fileId, 0))
	if err != nil {
		return inodeEntry{}, err
	}
	h := header{bufPage: headerPage}
	entry, err := allocateInodeEntry(mtr, fileId, h)
	if err != nil {
		return inodeEntry{}, err
	}
	segId := h.segId()
	h.setSegId(segId + 1)
	entry.initializeEntry(segId)
	if err := flst.InitBase(mtr, fileId, entry.freeListBase()); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	if err := flst.InitBase(mtr, fileId, entry.notFullListBase()); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	if err := flst.InitBase(mtr, fileId, entry.fullListBase()); err != nil {
		panicOnPostWriteFlstErr(err)
	}
	return entry, nil
}

// writeSegmentHeader は pageId のページの body 先頭から headerOffset の位置に inode エントリアドレスを書き込む
func writeSegmentHeader(mtr *buffer.Mtr, pageId page.Id, headerOffset uint16, entryAddr flst.Address) error {
	bufPage, err := mtr.PageForWrite(pageId)
	if err != nil {
		return err
	}
	writeAddressToPage(bufPage, int(headerOffset), entryAddr)
	return nil
}

// writeAddressToPage は bufPage の body 先頭から offset の位置に addr を 6 バイトで書き込む
func writeAddressToPage(bufPage *buffer.Page, offset int, addr flst.Address) {
	var buf [6]byte
	addr.WriteAt(buf[:], 0)
	bufPage.WriteBodyAt(offset, buf[:])
}
