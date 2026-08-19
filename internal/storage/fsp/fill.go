package fsp

import (
	"errors"
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// errCapacityExhausted はファイルの容量枯渇により extent を初期化できないことを表す
var errCapacityExhausted = errors.New("fsp: file capacity exhausted")

// fill はフリーリミットから extent の記述子を順に初期化し、FREE リストへ freeAddExtents 個追加した時点で停止する
//   - 記述子ページを含む extent は FREE_FRAG リストへ積まれ、freeAddExtents のカウント対象にはならない
//   - 完了時に論理ページ数がフリーリミットに満たなければ、フリーリミットまで引き上げる
//   - 初期化しようとする extent が sentinel PageNumber を含む場合は容量枯渇として error を返す
func fill(mtr *buffer.Mtr, fileId page.FileId) error {
	headerPage, err := mtr.PageForWrite(page.NewId(fileId, 0))
	if err != nil {
		return err
	}
	h := header{bufPage: headerPage}
	freeLimit := h.freeLimit()
	added := 0
	for added < freeAddExtents {
		firstPage := freeLimit
		if firstPage > page.MaxPageNumber-page.PageNumber(extentPageCount) {
			return fmt.Errorf("%w: cannot allocate extent starting at page %d", errCapacityExhausted, firstPage)
		}
		descrPageNum := descriptorPageNumber(firstPage)
		containsDescriptor := firstPage == descrPageNum
		if containsDescriptor && descrPageNum != 0 {
			if _, err := mtr.Pool().AddPage(page.NewId(fileId, descrPageNum)); err != nil {
				return err
			}
		}
		descrPage, err := mtr.PageForWrite(page.NewId(fileId, descrPageNum))
		if err != nil {
			return err
		}
		entry := xdesEntry{bufPage: descrPage, index: descriptorEntryIndex(firstPage)}
		entry.initialize()
		freeLimit += page.PageNumber(extentPageCount)
		h.setFreeLimit(freeLimit)
		if containsDescriptor {
			entry.setPageFree(0, false)
			entry.setState(stateFreeFrag)
			if err := flst.AddLast(mtr, fileId, h.freeFragListBase(), entry.flstNodeAddress()); err != nil {
				panicOnPostWriteFlstErr(err)
			}
			h.setFragNUsed(h.fragNUsed() + 1)
			continue
		}
		if err := flst.AddLast(mtr, fileId, h.freeListBase(), entry.flstNodeAddress()); err != nil {
			panicOnPostWriteFlstErr(err)
		}
		added++
	}
	if h.size() < uint32(freeLimit) {
		h.setSize(uint32(freeLimit))
	}
	return nil
}
