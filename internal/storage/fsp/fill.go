package fsp

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// errCapacityExhausted はファイルの容量枯渇により追加の extent を初期化できないことを表す
//   - 呼び出し側は fill 呼び出しの前後でフリーリミットが変化していないことでこの状態を検出し、
//     書き込みを開始する前に本エラーを返す
var errCapacityExhausted = errors.New("fsp: file capacity exhausted")

// fill はフリーリミットから extent の記述子を順に初期化し、FREE リストへ freeAddExtents 個追加した時点で停止する
//   - 記述子ページを含む extent は FREE_FRAG リストへ積まれ、freeAddExtents のカウント対象にはならない
//   - 完了時に論理ページ数がフリーリミットに満たなければ、フリーリミットまで引き上げる
//   - 残り空間が freeAddExtents 分に満たない場合は、追加できる分だけ追加して正常終了する (追加数が 0 でも正常)
func fill(mtr *buffer.Mtr, fileId page.FileId) error {
	headerPage, err := mtr.PageForWrite(page.NewId(fileId, 0))
	if err != nil {
		return err
	}
	h := header{bufPage: headerPage}
	plan := planFillExtents(h.freeLimit())
	if err := prefetchFillDescriptorPages(mtr, fileId, h, plan); err != nil {
		return err
	}
	executeFillPlan(mtr, fileId, h, plan)
	return nil
}

// planFillExtents は freeAddExtents 個の FREE 追加を目標に初期化対象の extent 先頭 PageNumber を確定する
//   - 容量 sentinel に到達した時点で切り上げる (返却スライスの長さは 0 の場合もある)
//   - 記述子ページを含む extent は追加数にカウントしない
func planFillExtents(freeLimit page.PageNumber) []page.PageNumber {
	plan := make([]page.PageNumber, 0, freeAddExtents+1)
	added := 0
	firstPage := freeLimit
	for added < freeAddExtents {
		if firstPage > page.MaxPageNumber-page.PageNumber(extentPageCount) {
			break
		}
		plan = append(plan, firstPage)
		if firstPage != descriptorPageNumber(firstPage) {
			added++
		}
		firstPage += page.PageNumber(extentPageCount)
	}
	return plan
}

// prefetchFillDescriptorPages は plan の各 extent が要求する記述子ページと、
// 書き込みフェーズで AddLast する遷移先リスト (FREE_FRAG / FREE) の旧末尾ページをすべて pin する
//   - 記述子ページ自身を含む extent (page 0 を除く) は AddPage で新規に作成する
//   - 以降の書き込みフェーズはここで pin されたページのみを更新する
func prefetchFillDescriptorPages(mtr *buffer.Mtr, fileId page.FileId, h header, plan []page.PageNumber) error {
	if len(plan) == 0 {
		return nil
	}
	for _, firstPage := range plan {
		descrPageNum := descriptorPageNumber(firstPage)
		if firstPage == descrPageNum && descrPageNum != 0 {
			if _, err := mtr.Pool().AddPage(page.NewId(fileId, descrPageNum)); err != nil {
				return err
			}
		}
		if _, err := mtr.PageForWrite(page.NewId(fileId, descrPageNum)); err != nil {
			return err
		}
	}
	freeFragLast, err := flst.Last(mtr, fileId, h.freeFragListBase())
	if err != nil {
		return err
	}
	if !freeFragLast.IsInvalid() {
		if _, err := mtr.PageForWrite(page.NewId(fileId, freeFragLast.PageNumber)); err != nil {
			return err
		}
	}
	freeLast, err := flst.Last(mtr, fileId, h.freeListBase())
	if err != nil {
		return err
	}
	if !freeLast.IsInvalid() {
		if _, err := mtr.PageForWrite(page.NewId(fileId, freeLast.PageNumber)); err != nil {
			return err
		}
	}
	return nil
}

// executeFillPlan は事前に pin 済みのページに対して初期化・リスト連結の書き込みを行う
//   - plan が空 (追加 0 個) の場合は何も書き込まない
//   - この段階では新規ページ取得を行わず、リスト操作の失敗は到達不能条件として panic に変換する
func executeFillPlan(mtr *buffer.Mtr, fileId page.FileId, h header, plan []page.PageNumber) {
	if len(plan) == 0 {
		return
	}
	freeLimit := h.freeLimit()
	for _, firstPage := range plan {
		descrPageNum := descriptorPageNumber(firstPage)
		descrPage, err := mtr.PageForWrite(page.NewId(fileId, descrPageNum))
		if err != nil {
			panic("fsp: descriptor page must be pinned before fill write phase")
		}
		entry := xdesEntry{bufPage: descrPage, index: descriptorEntryIndex(firstPage)}
		entry.initialize()
		freeLimit += page.PageNumber(extentPageCount)
		h.setFreeLimit(freeLimit)
		if firstPage == descrPageNum {
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
	}
	if h.size() < uint32(freeLimit) {
		h.setSize(uint32(freeLimit))
	}
}
